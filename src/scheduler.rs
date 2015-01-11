use std::sync::atomic::{AtomicUint, Ordering};
use std::thunk::Thunk;
use std::sync::{Arc,Mutex,TaskPool};
use std::thread::Thread;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::vec::Vec;

/// Trait used for generic scheduling of work
pub trait Scheduler : Clone + Send
{  
    /// Schedule `f` to be run as soon as possible by this scheduler.
    ///
    /// This function will typically return before `f` has actually run.
    fn schedule<F>(&self, f : F) where F : Send + FnOnce();
}

/// Scheduler that immediatly runs the function in the same thread 
///
/// Gives no parallelism, but has low overhead.
#[derive(Copy,Clone)]
pub struct SequentialScheduler;

/// Scheduler that spawns a new OS thread for each scheduled function
///
/// Gives maximum parallelism, but has high overhead.
#[derive(Copy,Clone)]
pub struct SpawningScheduler;

/// Scheduler that queues all scheduled functions to allow for inspection and later running them in a controlled manner, e.g. in tests
#[derive(Clone)]
pub struct TestScheduler
{
    scheduler : Sender<Thunk>,
    scheduled : Arc<Mutex<Receiver<Thunk>>>,
    scheduled_count : Arc<AtomicUint>
}

unsafe impl Send for SequentialScheduler {}
unsafe impl Send for SpawningScheduler {}
unsafe impl Send for TestScheduler {}

impl TestScheduler
{
    /// Creates a new test scheduler with its own queue
    pub fn new() -> TestScheduler
    {
        let (tx,rx) = channel();
        TestScheduler
        {
            scheduler : tx,
            scheduled : Arc::new(Mutex::new(rx)),
            scheduled_count : Arc::new(AtomicUint::new(0))
        }
    }

    /// Current count of functions that has been scheduled since creation or last call to `run_queued`
    pub fn queued_count(&self) -> usize
    {
        self.scheduled_count.load(Ordering::Relaxed)
    }

    /// Run all queued functions.
    ///
    /// If more functions are scheduled as a result of these functions being run, the new scheduled functions will be queued
    /// and another call to `run_queued` will need to be made to run them.
    pub fn run_queued(&self)
    {
        let mut v = Vec::new();
        while let Ok(t) = self.scheduled.lock().unwrap().try_recv()
        {
            v.push(t);
        }       
        self.scheduled_count.store(0, Ordering::Relaxed);
        for t in v.into_iter()
        {
            t.invoke(());
        }
    }

    /// Run all queued functions, and functions queued by that, recursively
    ///
    /// If more functions are scheduled as a result of these functions being run, they will also be run by this.
    pub fn run_queued_recursive(&self)
    {
        while self.queued_count() > 0
        {
            self.run_queued()
        }        
    }
}

impl Scheduler for SequentialScheduler
{
    /// Immediatly run `f` in the current thread
    fn schedule<F>(&self, f : F) where F : Send + FnOnce()
    {
        f()
    }
}

impl Scheduler for SpawningScheduler
{
    /// Spawn a new (detached) thread that runs `f`
    fn schedule<F>(&self, f : F) where F : Send + FnOnce()
    {
        Thread::spawn(f);
    }
}

impl Scheduler for TestScheduler
{
    /// Queue the function to be run later when `run_queued` is called
    fn schedule<F>(&self, f : F) where F : Send + FnOnce()
    {
        let _ = self.scheduler.send(Thunk::new(f));
        self.scheduled_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Scheduler for Arc<Mutex<TaskPool>>
{
    /// Execute `f` on the task pool
    fn schedule<F>(&self, f : F) where F : Send + FnOnce()
    {
        self.lock().unwrap().execute(f)
    }
}

