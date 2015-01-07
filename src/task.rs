use scheduler::{Scheduler, SequentialScheduler};
use std::sync::mpsc::{sync_channel, channel, Sender, Receiver};
use std::sync::atomic::{AtomicInt, Ordering};
use std::sync::{Arc,Mutex,Future, MutexGuard};
use std::thunk::Thunk;
//use std::vec::Vec;


struct SharedDependency(AtomicInt, Mutex<Receiver<Thunk>>);

fn run_dependency(arc: Arc<SharedDependency>)
{
    let SharedDependency(ref n, ref thunk) = *arc;
    if n.fetch_sub(1, Ordering::SeqCst) == 1
    {
        thunk.lock().unwrap().recv().unwrap().invoke(());
    }
}

fn make_shared_dependency<F>(wait_count : int, f: F) -> Arc<SharedDependency>
    where F : Send + FnOnce()
{
    let (tx, rx) = sync_channel(1u);
    let _ = tx.send(Thunk::new(f));
    Arc::new(SharedDependency(AtomicInt::new(wait_count), Mutex::new(rx)))
}

fn consume_result<A : Send>(mut value : A, dep_rec : Receiver<Thunk<A,Option<A>>>, mut cell : MutexGuard<Option<A>>)
{
    while let Ok(t) = dep_rec.try_recv()
    {
        if let Some(v) = t.invoke(value)
        {
            value = v;
        }
        else
        {
            return;
        }
    }
    *cell = Some(value);
}


//------------------------------------------------------------------------------------

/// A computation that eventually will produce a value that other tasks can depend on.
///
/// A graph of dependent tasks can be created by chaining together several tasks with
/// then and join. Parallelism is achived when multiple tasks depend on the same cloned task. 
/// Cloning a task will clone the produced value, but not run the function twice. If the produced
/// value is not `Clone`, the function `arc` can be used to wrap the value in an `Arc` (but will
/// then require the original value to be `Sync`).
/// There is virtually no locking involved; tasks will simply not be scheduled until 
/// all dependent tasks are done. A task can be converted into a `Future` in order to
/// allow blocking for a task to finish and to retrieve its result.
///
/// The result from the computation will need to be `Send`. `Task` implements `Send` itself and can thus
/// be used inside and returned from other tasks (effectively forming a monad).
pub struct Task<A : Send >
{
    dependencies : Sender<Thunk<A,Option<A>>>,
    result : Arc<Mutex<Option<A>>>
}

unsafe impl<A : Send> Send for Task<A> {}

impl<A : Send> Task<A>
{
    /// Create a finished task from a value
    pub fn from_value(val : A) -> Task<A>
    {
        let (dependencies, _) = channel();
        Task { dependencies : dependencies, result : Arc::new(Mutex::new(Some(val)))}
    }

    /// Create a task from a function that will be scheduled to run in `sched`.
    pub fn from_fn<F,S>(sched : &S, fun : F) -> Task<A>
        where F : Send + FnOnce() -> A,
              S : Scheduler
    {
        let result = Arc::new(Mutex::new(None));
        let result_clone = result.clone();
        let (dependencies, dep_rec) = channel();
        let f = move |:| { consume_result(fun(), dep_rec, result_clone.lock().unwrap()) };
        sched.schedule(f);
        Task { dependencies : dependencies, result : result}
    }

    /// Create a task that never finishes
    ///
    /// Tasks made dependent on this task will never be scheduled or run.
    pub fn never() -> Task<A>
    {
        let (dependencies, _) = channel();
        Task { dependencies : dependencies, result : Arc::new(Mutex::new(None))}
    }

    /// Create a task that is a continuation from this task
    ///
    /// This continuation will run in parallel with other continuations created from clones of the same task.
    /// The function `fun` will be scheduled on `sched` as soon as the current task that the 
    /// continuation was created from is finished.
    pub fn then<B : Send,F,S>(self, sched : &S, fun : F) -> Task<B>
        where F : Send + FnOnce(A) -> B,
              S : Scheduler
    {
        let result = Arc::new(Mutex::new(None));
        let result_clone = result.clone();
        let (dependencies, dep_rec) = channel();
        let f = move |: a| { consume_result(fun(a), dep_rec, result_clone.lock().unwrap()) };

        let mut lock = self.result.lock().unwrap();
        match lock.take()
        {
            Some(a) => sched.schedule(move |:| { f(a) }),
            None => 
            {
                let sched_clone = sched.clone();
                let _ = self.dependencies.send(Thunk::with_arg(move |: a| { sched_clone.schedule(move |:| {f(a)}); None }));
            }
        }

        Task { dependencies : dependencies, result : result}
    }

    /// Run a function as a continuation from this task, but don't return it in a task
    ///
    /// This continuation will run in parallel with other continuations created from clones of the same task.
    /// The function `fun` will be scheduled on `sched` as soon as the current task that the 
    /// continuation was created from is finished.
    pub fn then_forget<F,S>(self, sched : &S, fun : F)
        where F : Send + FnOnce(A),
              S : Scheduler
    {
        let mut lock = self.result.lock().unwrap();
        match lock.take()
        {
            Some(a) => sched.schedule(move |:| { fun(a) }),
            None => 
            {
                let sched_clone = sched.clone();
                let _ = self.dependencies.send(Thunk::with_arg(move |: a| { sched_clone.schedule(move |:| {fun(a)}); None }));
            }
        }
    }

    /// Create a task that contains the result of this and another task.
    ///
    /// Continuations on this task will run in parallel with other continuations created from clones of these tasks.
    /// The function `fun` will be scheduled on `sched` as soon as both the current task and the other
    /// task that the continuation was created from are finished. The function `fun` will retrieve both
    /// parent tasks output as arguments.
	pub fn join<B:Send>(self, other : Task<B>) -> Task<(A,B)>
	{
        let result = Arc::new(Mutex::new(None));
        let (dependencies, dep_rec) = channel();
        let mut a_lock = self.result.lock().unwrap();
        let mut b_lock = other.result.lock().unwrap();

        match (a_lock.take(), b_lock.take())
        {
            (None,None) =>
            {
                // Both not done
                let (tx_a, rx_a) = sync_channel(1);
                let (tx_b, rx_b) = sync_channel(1);
                let result_clone = result.clone();
                let shared_dep = make_shared_dependency(2, move |:| { consume_result((rx_a.recv().unwrap(), rx_b.recv().unwrap()), dep_rec, result_clone.lock().unwrap()); });
                let shared_dep2 = shared_dep.clone();

                let _ = self.dependencies.send(Thunk::with_arg(move |: a| { let _ = tx_a.send(a); run_dependency(shared_dep); None } ));
                let _ = other.dependencies.send(Thunk::with_arg(move |: b| { let _ = tx_b.send(b); run_dependency(shared_dep2); None } ));
            }
            (None, Some(b)) =>
            {
                // a not done
                let result_clone = result.clone();
                let f = move |:a| { consume_result((a, b), dep_rec, result_clone.lock().unwrap()); None };
                let _ = self.dependencies.send(Thunk::with_arg(f));
            }
            (Some(a), None) =>
            {
                // b not done
                let result_clone = result.clone();
                let f = move |:b| { consume_result((a, b), dep_rec, result_clone.lock().unwrap()); None };
                let _ = other.dependencies.send(Thunk::with_arg(f));
            }
            (Some(a), Some(b)) =>
            {
                // Both done
                *result.lock().unwrap() = Some((a, b));
            }
        }

        Task { dependencies : dependencies, result : result}
	}

    /// Turn this task into a future, from which the result can be retrieved
    pub fn into_future(self) -> Future<A>
    {
        let (tx, rx) = sync_channel(1);
        self.then_forget(&SequentialScheduler, move |: a| { let _ = tx.send(a); });
        Future::from_receiver(rx)
    }

    /// Returns if this task has finished
    ///
    /// Mostly only useful for testing, or together with waiting for a future returned from `into_future`
    pub fn is_done(& self) -> bool
    {
        self.result.lock().unwrap().is_some()
    }
}

impl<A : Send + Clone> Clone for Task<A>
{
    /// Clones the task into another task that contains a clone of the original tasks value when the original task completes
    ///
    /// The cloned task will be done when the original task is, and no other computation will be scheduled
    fn clone(&self) -> Task<A>
    {
        let lock = self.result.lock().unwrap();
        match *lock
        {
            Some(ref a) => Task { dependencies : self.dependencies.clone(), result : Arc::new(Mutex::new(Some(a.clone())))},
            None => 
                {
                let (dependencies, dep_rec) = channel();
                let result = Arc::new(Mutex::new(None));
                let result_clone = result.clone();
                let f = move |: a : A| { consume_result(a.clone(), dep_rec, result_clone.lock().unwrap()); Some(a) };
                let _ = self.dependencies.send(Thunk::with_arg(f));
                Task { dependencies : dependencies, result : result}
            }
        }
    }
}

impl<A : Send + Sync> Task<A>
{
    pub fn arc(self) -> Task<Arc<A>>
    {
        self.then(&SequentialScheduler, |: a| { Arc::new(a) })
    }
}

impl<A : Send> Task<Task<A>>
{
    /// Retrieve the value from the inner task
    ///
    /// This is removing one layer of `Task` without blocking, i.e. the resulting 
    /// task will not run until the inner task is finished.
    pub fn unwrap(self) -> Task<A>
    {
        let result = Arc::new(Mutex::new(None));
        let result_clone = result.clone();
        let (dependencies, dep_rec) = channel();
        let f = move |: t : Task<A> | { 
            t.then_forget(&SequentialScheduler, move |: a| { consume_result(a, dep_rec, result_clone.lock().unwrap()) })
        };

        self.then_forget(&SequentialScheduler, f);

        Task { dependencies : dependencies, result : result}
    }
}

/*
/// Create a task that runs as a continuation of mutliple other tasks
///
/// This continuation will run in parallel with other continuations created from these tasks.
/// The function `fun` will be scheduled on `sched` as soon as all the tasks that the continuation was created from are
/// finished. The function `fun` will retrieve all the parent tasks output as arguments.
/// This function works even with an empty slice of input tasks, in which case it is scheduled immediatly (with an empty
/// slice as input)

trait JoinAll<A : Send>
{
    pub fn join_all(self) -> Task<Vec<A>>;
}

impl<A : Send> JoinAll<A> for Iterator<Task<A>>
{
    pub fn join_all(self) -> Task<Vec<A>>
    {
        panic!("Not impl");
    }
}
*/
/*
pub fn join_all<A:Send, F, S>(sched : &S, tasks : &[Task<A>], fun : F) -> Task<B>
    where F : Send + FnOnce(&[A]) -> B,
          S : Scheduler
{
    let result = Arc::new(Mutex::new(None));
    let (dependencies, dep_rec) = channel();

    let mut a_lock = self.result.lock().unwrap();
    let mut b_lock = other.result.lock().unwrap();

    match (a_lock.take(), b_lock.take())
    {
        (None,None) =>
        {
            // Both not done
            let (tx_a, rx_a) = sync_channel(1);
            let (tx_b, rx_b) = sync_channel(1);
            let result_clone = result.clone();
            let f = move |:| { consume_result((rx_a.recv(), rx_b.recv()), dep_rec, result_clone.lock().unwrap()) };
            let shared_dep = make_shared_dependency(2, f);
            let shared_dep2 = shared_dep.clone();

            self.dependencies.send(Thunk::with_arg(move |: a| { tx_a.send(a); run_dependency(shared_dep); None } ));
            other.dependencies.send(Thunk::with_arg(move |: b| { tx_b.send(b); run_dependency(shared_dep2); None } ));
        }
        (None, Some(b)) =>
        {
            // a not done
            let result_clone = result.clone();
            let f = move |:a| { consume_result((a, b), dep_rec, result_clone.lock().unwrap()); None };
            self.dependencies.send(Thunk::with_arg(f));
        }
        (Some(a), None) =>
        {
            // b not done
            let result_clone = result.clone();
            let f = move |:b| { consume_result((a, b), dep_rec, result_clone.lock().unwrap()); None };
            other.dependencies.send(Thunk::with_arg(f));
        }
        (Some(a), Some(b)) =>
        {
            // Both done
            *result.lock().unwrap() = Some((a, b));
        }
    }

    Task { dependencies : dependencies, result : result}
}
*/

/*
/// Create a task that runs as a continuation of one of mutliple other tasks
///
/// This continuation will run in parallel with other continuations created from these tasks.
/// The function `fun` will be scheduled on `sched` as soon as at least one the tasks that the continuation was created from are
/// finished. The function `fun` will retrieve the output from one of the tasks as argument.
///
/// Running `join_any` on an empty slice, will result in a task that never runs. 
pub fn join_any<A:Send, B:Send, F, S>(sched : &S, tasks : &[Task<A>], fun : F) -> Task<B>
    where F : Send + FnOnce(A) -> B,
          S : Scheduler
{
    let (tx, rx) = sync_channel(1);
    let result = Arc::new(Mutex::new(None));
    let result_clone = result.clone();
    let (dependencies, deps_rec) = channel();

    let mut values = Vec::with_capacity(tasks.len());
    let mut guards = Vec::with_capacity(tasks.len());
    let mut prereqs = Vec::with_capacity(tasks.len());

    let mut ready = false;
    for task in tasks.iter()
    {
        let guard = task.result.lock().unwrap();
        ready = *guard;
        guards.push(guard);
        values.push(task.value);
        prereqs.push(&task.dependencies);
        if ready
        {
            break;
        }
    }   
    let f = move |:| {
        for v in values.iter()
        {           
            if let Ok(x) = v.try_recv()
            {
                fun(x);
                let lock = result_clone.lock().unwrap();
                *lock = true;
                run_dependencies(rx);
                break;
            }
        }
    };
    setup_prereqs_ex(ready, 1i, prereqs, sched, f);

    Task { dependencies : dependencies, value : rx, result : result}
}
*/
#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::TestScheduler;

    #[test]
    fn task_from_value_is_done() {
        let t = Task::from_value(1i);
        assert!(t.is_done());
        assert_eq!(t.into_future().get(), 1i);
    }

    #[test]
    fn task_from_fn_is_scheduled() {
        let sched = TestScheduler::new();

        let t = Task::from_fn(&sched, || { 1i } );

        assert!(!t.is_done());
        assert_eq!(sched.queued_count(), 1);
    }

    #[test]
    fn task_from_fn_returns() {
        let sched = TestScheduler::new();

        let t = Task::from_fn(&sched, || { 1i } );

        sched.run_queued();

        assert!(t.is_done());
        assert_eq!(sched.queued_count(), 0);

        assert_eq!(t.into_future().get(),1);
    }

    #[test]
    fn never_task_is_not_done() {
        let t = Task::<int>::never();
        assert!(!t.is_done());
    }

    #[test]
    fn then_of_done_task_scheduled() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b = a.then(&sched, |x| {x + 2} );

        assert!(!b.is_done());
        assert_eq!(sched.queued_count(), 1);
    }


    #[test]
    fn parallel_thens_scheduled_in_parallel() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.clone().then(&sched, |x| {x + 2} );
        let b2 = a.then(&sched, |x| {x + 4} );

        assert!(!b1.is_done());
        assert!(!b2.is_done());
        assert_eq!(sched.queued_count(), 2);
    }

    #[test]
    fn parallel_thens_returns() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.clone().then(&sched, |x| {x + 2} );
        let b2 = a.then(&sched, |x| {x + 4} );

        sched.run_queued();

        assert!(b1.is_done());
        assert!(b2.is_done());
        assert_eq!(sched.queued_count(), 0);

        assert_eq!(b1.into_future().get(),3);
        assert_eq!(b2.into_future().get(),5);
    }

    #[test]
    fn then_of_then_not_scheduled() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b = a.then(&sched, |x| {x + 2} );
        let c = b.then(&sched, |x| {x + 4} );

        assert!(!c.is_done());
        assert_eq!(sched.queued_count(), 1);
    }

    #[test]
    fn finished_then_schedules_next_then() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b = a.then(&sched, |x| {x + 2} );
        let c = b.clone().then(&sched, |x| {x + 4} );

        sched.run_queued();
        
        assert!(b.is_done());
        assert!(!c.is_done());
        assert_eq!(sched.queued_count(), 1);
    }

    #[test]
    fn parallel_thens_of_parallel_thens_scheduled_in_parallel() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.clone().then(&sched, |x| {x + 2} );
        let b2 = a.then(&sched, |x| {x + 4} );

        let c1 = b1.clone().then(&sched, |x| {x + 8} );
        let c2 = b1.then(&sched, |x| {x + 16} );
        let c3 = b2.clone().then(&sched, |x| {x + 32} );
        let c4 = b2.then(&sched, |x| {x + 64} );
        sched.run_queued();
 
        assert!(!c1.is_done());
        assert!(!c2.is_done());
        assert!(!c3.is_done());
        assert!(!c4.is_done());

        assert_eq!(sched.queued_count(), 4);
    }

    #[test]
    fn parallel_thens_of_parallel_thens_returns() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.clone().then(&sched, |x| {x + 2} );
        let b2 = a.then(&sched, |x| {x + 4} );

        let c1 = b1.clone().then(&sched, |x| {x + 8} );
        let c2 = b1.then(&sched, |x| {x + 16} );
        let c3 = b2.clone().then(&sched, |x| {x + 32} );
        let c4 = b2.then(&sched, |x| {x + 64} );

        sched.run_queued();
        sched.run_queued();

        assert!(c1.is_done());
        assert!(c2.is_done());
        assert!(c3.is_done());
        assert!(c4.is_done());
        assert_eq!(sched.queued_count(), 0);

        assert_eq!(c1.into_future().get(), 1 + 2 + 8);
        assert_eq!(c2.into_future().get(), 1 + 2 + 16);
        assert_eq!(c3.into_future().get(), 1 + 4 + 32);
        assert_eq!(c4.into_future().get(), 1 + 4 + 64);
    }

    #[test]
    fn join_is_not_scheduled_before_both_parents_are_done() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched2, || {2i} );

        let c = a.clone().join(b.clone()).then(&sched3, |(x,y)| { x + y });

        sched1.run_queued();

        assert!(a.is_done());
        assert!(!b.is_done());
        assert!(!c.is_done());

        assert_eq!(sched3.queued_count(), 0);
    }

    #[test]
    fn join_is_scheduled_when_both_parents_are_done() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched2, || {2i} );

        let c = a.clone().join(b.clone()).then(&sched3, |(x,y)| { x + y });

        sched1.run_queued();
        sched2.run_queued();

        assert!(a.is_done());
        assert!(b.is_done());
        assert!(!c.is_done());

        assert_eq!(sched3.queued_count(), 1);
    }

    #[test]
    fn join_returns() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched2, || {2i} );

        let c = a.join(b).then(&sched3, |(x,y)| { x + y });

        sched1.run_queued();
        sched2.run_queued();
        sched3.run_queued();

        assert!(c.is_done());
        assert_eq!(c.into_future().get(), 3i);
    }
/*
    #[test]
    fn join_all_is_not_scheduled_before_all_parents_are_done() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched1, || {2i} );
        let c = Task::from_fn(&sched2, || {4i} );

        let d = join_all(&sched3, &[a.clone(),b.clone(),c.clone()], |xs| { xs[0] + xs[1] + xs[2] });

        sched1.run_queued();

        assert!(a.is_done());
        assert!(b.is_done());
        assert!(!c.is_done());
        assert!(!d.is_done());

        assert_eq!(sched3.queued_count(), 0);
    }

    #[test]
    fn join_all_is_scheduled_when_all_parents_are_done() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();


        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched1, || {2i} );
        let c = Task::from_fn(&sched2, || {4i} );

        let d = join_all(&sched3, &[a,b,c], |xs| { xs[0] + xs[1] + xs[2] });

        sched1.run_queued();
        sched2.run_queued();

        assert!(a.is_done());
        assert!(b.is_done());
        assert!(c.is_done());
        assert!(!d.is_done());

        assert_eq!(sched3.queued_count(), 1);
    }

    #[test]
    fn join_all_returns() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched1, || {2i} );
        let c = Task::from_fn(&sched2, || {4i} );

        let d = join_all(&sched3, &[a,b,c], |xs| { xs[0] + xs[1] + xs[2] });

        sched1.run_queued();
        sched2.run_queued();
        sched3.run_queued();

        assert!(d.is_done());
        assert_eq!(d.into_future().get(), 7i);
    }

    #[test]
    fn join_any_is_not_scheduled_before_one_parent_is_done() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched1, || {2i} );
        let c = Task::from_fn(&sched2, || {4i} );

        let d = join_any(&sched3, &[&a,&b,&c], |xs| { xs + 8i });

        assert!(!a.is_done());
        assert!(!b.is_done());
        assert!(!c.is_done());
        assert!(!d.is_done());

        assert_eq!(sched3.queued_count(), 0);
    }

    #[test]
    fn join_any_is_scheduled_when_one_parent_is_done() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched1, || {2i} );
        let c = Task::from_fn(&sched2, || {4i} );

        let d = join_any(&sched3, &[&a,&b,&c], |xs| { *xs + 8i });

        sched2.run_queued();

        assert!(!a.is_done());
        assert!(!b.is_done());
        assert!(c.is_done());
        assert!(!d.is_done());

        assert_eq!(sched3.queued_count(), 1);
    }

    #[test]
    fn join_any_returns() {
        let sched1 = TestScheduler::new();
        let sched2 = TestScheduler::new();
        let sched3 = TestScheduler::new();

        let a = Task::from_fn(&sched1, || {1i} );
        let b = Task::from_fn(&sched1, || {2i} );
        let c = Task::from_fn(&sched2, || {4i} );

        let d = join_any(&sched3, &[&a,&b,&c], |xs| { *xs + 8i });

        sched2.run_queued();
        sched3.run_queued();

        assert!(d.is_done());
        assert_eq!(d.into_future().get(), 12i);
    }*/

    fn sort(s : &TestScheduler, mut xs : Vec<int>) -> Task<Vec<int>>
    {
        if xs.len() <= 1
        {
            return Task::from_value(xs);
        }

        let pivot = xs.pop().unwrap();
        let (a,b) = xs.into_iter().partition(|&n| n < pivot);

        let s1 = s.clone();
        let s2 = s.clone();
        let x = Task::from_fn(s, move ||{ sort(&s1,a) }).unwrap();
        let y = Task::from_fn(s, move ||{ sort(&s2,b) }).unwrap();
        x.join(y).then(s, move |(mut a,b)| { a.push(pivot); a.extend(b.into_iter()); a })
    }

    #[test]
    fn can_loop() {
        let sched = TestScheduler::new();
        let unsorted = vec![6,2,3,1,6,3,4];
        let mut sorted = sort(&sched, unsorted).into_future();
        sched.run_queued_recursive();
        assert_eq!(vec![1, 2, 3, 3, 4, 6, 6], sorted.get());
    }
}
