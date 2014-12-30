use scheduler::{Scheduler, SequentialScheduler};
use std::sync::atomic::{AtomicInt, Ordering};
use std::thunk::Thunk;
use std::sync::{RWLock,Arc,Mutex,Future};
use std::comm::{channel, Sender, Receiver};
use std::vec::Vec;

struct SharedDependency(AtomicInt, Mutex<Receiver<Thunk>>);

fn run_dependencies(rx: Receiver<Arc<SharedDependency>>)
{
    while let Ok(arc) = rx.try_recv()
    {
        let SharedDependency(ref n, ref thunk) = *arc;
        if n.fetch_sub(1, Ordering::SeqCst) == 1
        {
            thunk.lock().recv().invoke(());
        }
    }    
}

fn setup_prereqs_ex<F,S>(ready : bool, wait_count : int, prereqs : Vec<&Mutex<Sender<Arc<SharedDependency>>>>, sched : &S, f : F)
    where F : Send + FnOnce(),
          S : Scheduler
{
    if ready
    {
        sched.schedule(f);
    }
    else
    {
        let (tx, rx) = sync_channel(1u);
        let sched_clone = sched.clone();
        tx.send(Thunk::new(move || { sched_clone.schedule(f) }));
        let atom = Arc::new(SharedDependency(AtomicInt::new(wait_count), Mutex::new(rx)));
        for prereq in prereqs.iter()
        {
            let _ = prereq.lock().send_opt(atom.clone());
        }
    }
}

fn setup_prereqs<F,S>(prereqs : Vec<&Mutex<Sender<Arc<SharedDependency>>>>, sched : &S, f : F)
    where F : Send + FnOnce(),
          S : Scheduler
{
    let c = prereqs.len() as int;
    setup_prereqs_ex(c == 0, c, prereqs, sched, f);
}

//------------------------------------------------------------------------------------

/// A computation that eventually will produce a value that other tasks can depend on.
///
/// A graph of dependent tasks can be created by chaining together several tasks with
/// then and join. Parallelism is achived when multiple tasks depend on the same task.
/// There is virtually no locking involved; tasks will simply not be scheduled until 
/// all dependent tasks are done. A task can be converted into a `Future` in order to
/// allow blocking for a task to finish and to retrieve its result.
///
/// The result from the computation is buffered in the task and shared between dependent 
/// tasks, meaning that they will get a borrowed `&A` from a `Task<A>`, and `A` will need
/// to be both `Send` (for being send to other threads) and `Sync` (for being read concurrently
/// by multiple dependent tasks). `Task` itself implements these traits itself and can thus
/// be used inside and returned from other tasks (effectively forming a monad).
#[deriving(Send,Sync)]
pub struct Task<A>
{
    deps_after : Mutex<Sender<Arc<SharedDependency>>>,
    value : Arc<RWLock<Option<A>>>
}

impl<A : Send + Sync> Task<A>
{
    /// Create a finished task from a value
    pub fn from_value(val : A) -> Task<A>
    {
        let (deps_after, _) = channel();
        Task { deps_after : Mutex::new(deps_after), value : Arc::new(RWLock::new(Some(val)))}
    }

    /// Create a task from a function that will be scheduled to run in `sched`.
    pub fn from_fn<F,S>(sched : &S, fun : F) -> Task<A>
        where F : Send + FnOnce() -> A,
              S : Scheduler
    {
        let value = Arc::new(RWLock::new(None));
        let (deps_after, rx) = channel();
        let out_val = value.clone();
        let f = move |:| { 
            let out = fun();
            {
                *out_val.write() = Some(out);
            }
            run_dependencies(rx);
        };
        sched.schedule(f);
        Task { deps_after : Mutex::new(deps_after), value : value}
    }

    /// Create a task that runs a function as a continuation from this task
    ///
    /// Tasks made dependent on this task will never run.
    /// If this or a dependent task is turned into a `Future`, forcing the value of
    /// that future will cause a panic (instead of an infinite loop).
    pub fn never() -> Task<A>
    {
        let (deps_after, _) = channel();
        Task { deps_after : Mutex::new(deps_after), value : Arc::new(RWLock::new(None))}
    }

    /// Create a task that is a continuation from this task
    ///
    /// This continuation will run in parallel with other continuations created from the same task.
    /// The function `fun` will be scheduled on `sched` as soon as the current task that the 
    /// continuation was created from is finished.
    pub fn then<B : Send + Sync,F,S>(&self, sched : &S, fun : F) -> Task<B>
        where F : Send + FnOnce(&A) -> B,
              S : Scheduler
    {
        let value = Arc::new(RWLock::new(None));
        let (deps_after, rx) = channel();
        let in_val = self.value.clone();
        let out_val = value.clone();
        let f = move |:| { 
            let out = fun(in_val.read().as_ref().unwrap());
            {
                *out_val.write() = Some(out);
            }
            run_dependencies(rx);
        };
        let rguard = self.value.read();
        let mut prereq = Vec::new();
        if rguard.is_none() 
        {
            prereq.push(&self.deps_after);
        }
        setup_prereqs(prereq, sched, f);
        Task { deps_after : Mutex::new(deps_after), value : value}
    }

    /// Run a function as a continuation from this task, but don't return it in a task
    ///
    /// This continuation will run in parallel with other continuations created from the same task.
    /// The function `fun` will be scheduled on `sched` as soon as the current task that the 
    /// continuation was created from is finished.
    pub fn then_forget<F,S>(&self, sched : &S, fun : F)
        where F : Send + FnOnce(&A),
              S : Scheduler
    {
        let in_val = self.value.clone();
        let f = move |:| { fun(in_val.read().as_ref().unwrap()) };
        let rguard = self.value.read();
        let mut prereq = Vec::new();
        if rguard.is_none() 
        {
            prereq.push(&self.deps_after);
        }
        setup_prereqs(prereq, sched, f);
    }

    /// Create a task that runs a function as a continuation from this and another task
    ///
    /// This continuation will run in parallel with other continuations created from these tasks.
    /// The function `fun` will be scheduled on `sched` as soon as both the current task and the other
    /// task that the continuation was created from are finished. The function `fun` will retrieve both
    /// parent tasks output as arguments.
	pub fn join<B:Send+Sync, C:Send+Sync, F, S>(&self, sched : &S, other : &Task<B>, fun : F) -> Task<C>
	    where F : Send + FnOnce(&A, &B) -> C,
	          S : Scheduler
	{
	    let value = Arc::new(RWLock::new(None));
	    let (deps_after, rx) = channel();
	    let a_in_val = self.value.clone();
	    let b_in_val = other.value.clone();
	    let out_val = value.clone();
	    let f = move |:| { 
	        let out = fun(a_in_val.read().as_ref().unwrap(),b_in_val.read().as_ref().unwrap());
            {
    	        *out_val.write() = Some(out);
            }
	        run_dependencies(rx);
	    };

	    let a_rguard = self.value.read();
	    let b_rguard = other.value.read();
	    let mut prereqs = Vec::with_capacity(2u);

	    if a_rguard.is_none()
	    {
	        prereqs.push(&self.deps_after);
	    }
	    if b_rguard.is_none()
	    {
	        prereqs.push(&other.deps_after);
	    }

	    setup_prereqs(prereqs, sched, f);
	    Task { deps_after : Mutex::new(deps_after), value : value}
	}

    /// Turn this task into a future, from which the result can be retrieved
    ///
    /// Since the task's value is shared with other continuations, a function that turns the borrowed
    /// `&A` value into an owned B value is needed. If `A` implements `Clone`, then use 
    /// `copy_to_future` instead.
    pub fn to_future<B : Send+Sync,F,S>(& self, sched : &S, fun : F) -> Future<B>
        where F : Send + FnOnce(&A) -> B,
              S : Scheduler
    {
        let (tx, rx) = channel();
        self.then_forget(sched, move |a| { tx.send(fun(a))});
        Future::from_fn(move || { 
            match rx.recv_opt()
            {
                Ok(x) => x,
                _ => panic!("Forcing future on task that will never finish!") // This is awsome!
            }
        })
    }

    /// Returns if this task has finished
    ///
    /// Mostly only useful for testing, or together with waiting for a future returned from `to_future` or `copy_to_future`.
    pub fn is_done(& self) -> bool
    {
        self.value.read().is_some()
    }
}

impl<A : Send + Sync+ Clone> Task<A>
{
    /// Turn this task into a future, from which the result can be retrieved
    ///
    /// This version will clone the returned value into the future, so other dependent tasks may still use the 
    /// original value.
    pub fn copy_to_future(&self) -> Future<A>
    {
        self.to_future(&SequentialScheduler, |a| {a.clone()})
    }
}

impl<A : Send + Sync> Task<Task<A>>
{
    /// Retrieve the value from the inner task
    ///
    /// This is removing one layer of `Task` without blocking, i.e. the resulting 
    /// task will not run until the inner task is finished.
    /// Since the task's value is shared with other continuations, a function that turns the borrowed
    /// `&A` value into an owned B value is needed. If `A` implements `Clone`, then use 
    /// `unwrap_copy` instead.
    pub fn unwrap<B : Send + Sync, F, S>(&self, sched : &S, fun : F) -> Task<B>
        where F : Send + FnOnce(&A) -> B,
              S : Scheduler
    {
        let value = Arc::new(RWLock::new(None));
        let (deps_after, rx) = channel();
        let in_val = self.value.clone();
        let out_val = value.clone();
        let sched_clone = sched.clone();
        let f = move |:| { 
            in_val.read().as_ref().unwrap().then_forget(&sched_clone, move |a| {
                let out = fun(a);
                {
                    *out_val.write() = Some(out);
                }
                run_dependencies(rx);
            });
        };
        let rguard = self.value.read();
        let mut prereq = Vec::new();
        if rguard.is_none() 
        {
            prereq.push(&self.deps_after);       
        }
        setup_prereqs(prereq, &SequentialScheduler, f);
        Task { deps_after : Mutex::new(deps_after), value : value}
    }
}

impl<A : Send + Sync+ Clone> Task<Task<A>>
{
    /// Retrieve the value from the inner task
    ///
    /// This is removing one layer of `Task` without blocking, i.e. the resulting 
    /// task will not run until the inner task is finished.
    /// This version will clone the inner value into the task, so other dependent tasks may still use the 
    /// original value.
    pub fn unwrap_copy(&self) -> Task<A>
    {
        self.unwrap(&SequentialScheduler, |a| {a.clone()})
    }
}

/// Create a task that runs as a continuation of mutliple other tasks
///
/// This continuation will run in parallel with other continuations created from these tasks.
/// The function `fun` will be scheduled on `sched` as soon as all the tasks that the continuation was created from are
/// finished. The function `fun` will retrieve all the parent tasks output as arguments.
/// This function works even with an empty slice of input tasks, in which case it is scheduled immediatly (with an empty
/// slice as input)
pub fn join_all<A:Send+Sync, B:Send+Sync, F, S>(sched : &S, tasks : &[&Task<A>], fun : F) -> Task<B>
    where F : Send + FnOnce(&[&A]) -> B,
          S : Scheduler
{
    let value = Arc::new(RWLock::new(None));
    let (deps_after, rx) = channel();
    let out_val = value.clone();

    let mut values = Vec::with_capacity(tasks.len());
    let mut guards = Vec::with_capacity(tasks.len());
    let mut prereqs = Vec::with_capacity(tasks.len());
    for task in tasks.iter()
    {
        let guard = task.value.read();
        if guard.is_none()
        {
            prereqs.push(&task.deps_after);
        }
        guards.push(guard);
        values.push(task.value.clone());
    }
    let f = move |:| {
        let val_guards = values.iter().map(|val| val.read()).collect::<Vec<_>>();
        let out = fun(val_guards.iter().map(|val| val.as_ref().unwrap()).collect::<Vec<_>>().as_slice());
        {
            *out_val.write() = Some(out);
        }
        run_dependencies(rx);
    };   

    setup_prereqs(prereqs, sched, f);
    Task { deps_after : Mutex::new(deps_after), value : value}
}

/// Create a task that runs as a continuation of one of mutliple other tasks
///
/// This continuation will run in parallel with other continuations created from these tasks.
/// The function `fun` will be scheduled on `sched` as soon as at least one the tasks that the continuation was created from are
/// finished. The function `fun` will retrieve the output from one of the tasks as argument.
///
/// Running `join_any` on an empty slice, will result in a task that never runs. 
pub fn join_any<A:Send+Sync, B:Send+Sync, F, S>(sched : &S, tasks : &[&Task<A>], fun : F) -> Task<B>
    where F : Send + FnOnce(&A) -> B,
          S : Scheduler
{
    let value = Arc::new(RWLock::new(None));
    let (deps_after, rx) = channel();
    let out_val = value.clone();

    let mut values = Vec::with_capacity(tasks.len());
    let mut guards = Vec::with_capacity(tasks.len());
    let mut prereqs = Vec::with_capacity(tasks.len());

    let mut ready = false;
    for task in tasks.iter()
    {
        let guard = task.value.read();
        ready = guard.is_some();
        guards.push(guard);
        values.push(task.value.clone());
        prereqs.push(&task.deps_after);
        if ready
        {
            break;
        }
    }   
    let f = move |:| {
        for v in values.iter()
        {
            let guard = v.read();
            if guard.is_some()
            {
                let out = fun(guard.as_ref().unwrap());
                {
                    *out_val.write() = Some(out);
                }
                run_dependencies(rx);
                break;
            }
        }
    };
    setup_prereqs_ex(ready, 1i, prereqs, sched, f);

    Task { deps_after : Mutex::new(deps_after), value : value}
}

#[cfg(test)]
mod tests {
    use super::*;
    use scheduler::TestScheduler;

    #[test]
    fn task_from_value_is_done() {
        let t = Task::from_value(1i);
        assert!(t.is_done());
        assert_eq!(t.copy_to_future().get(), 1i);
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

        assert_eq!(t.copy_to_future().get(),1);
    }

    #[test]
    fn never_task_is_not_done() {
        let t = Task::<int>::never();
        assert!(!t.is_done());
    }

    #[test]
    fn parallel_thens_scheduled_in_parallel() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.then(&sched, |x| {*x + 2} );
        let b2 = a.then(&sched, |x| {*x + 4} );

        assert!(!b1.is_done());
        assert!(!b2.is_done());
        assert_eq!(sched.queued_count(), 2);
    }

    #[test]
    fn parallel_thens_returns() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.then(&sched, |x| {*x + 2} );
        let b2 = a.then(&sched, |x| {*x + 4} );

        sched.run_queued();

        assert!(b1.is_done());
        assert!(b2.is_done());
        assert_eq!(sched.queued_count(), 0);

        assert_eq!(b1.copy_to_future().get(),3);
        assert_eq!(b2.copy_to_future().get(),5);
    }

    #[test]
    fn then_of_then_not_scheduled() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b = a.then(&sched, |x| {*x + 2} );
        let c = b.then(&sched, |x| {*x + 4} );

        assert!(!b.is_done());
        assert!(!c.is_done());
        assert_eq!(sched.queued_count(), 1);
    }

    #[test]
    fn finished_then_schedules_next_then() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b = a.then(&sched, |x| {*x + 2} );
        let c = b.then(&sched, |x| {*x + 4} );

        sched.run_queued();
        
        assert!(b.is_done());
        assert!(!c.is_done());
        assert_eq!(sched.queued_count(), 1);
    }

    #[test]
    fn parallel_thens_of_parallel_thens_scheduled_in_parallel() {
        let sched = TestScheduler::new();

        let a = Task::from_value(1i);
        let b1 = a.then(&sched, |x| {*x + 2} );
        let b2 = a.then(&sched, |x| {*x + 4} );

        let c1 = b1.then(&sched, |x| {*x + 8} );
        let c2 = b1.then(&sched, |x| {*x + 16} );
        let c3 = b2.then(&sched, |x| {*x + 32} );
        let c4 = b2.then(&sched, |x| {*x + 64} );

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
        let b1 = a.then(&sched, |x| {*x + 2} );
        let b2 = a.then(&sched, |x| {*x + 4} );

        let c1 = b1.then(&sched, |x| {*x + 8} );
        let c2 = b1.then(&sched, |x| {*x + 16} );
        let c3 = b2.then(&sched, |x| {*x + 32} );
        let c4 = b2.then(&sched, |x| {*x + 64} );

        sched.run_queued();
        sched.run_queued();

        assert!(c1.is_done());
        assert!(c2.is_done());
        assert!(c3.is_done());
        assert!(c4.is_done());
        assert_eq!(sched.queued_count(), 0);

        assert_eq!(c1.copy_to_future().get(), 1 + 2 + 8);
        assert_eq!(c2.copy_to_future().get(), 1 + 2 + 16);
        assert_eq!(c3.copy_to_future().get(), 1 + 4 + 32);
        assert_eq!(c4.copy_to_future().get(), 1 + 4 + 64);
    }
}