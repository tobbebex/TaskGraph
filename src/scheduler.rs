use std::sync::atomic::{AtomicUint, Ordering};
use std::thunk::Thunk;
use std::sync::{Arc,Mutex};
use std::thread::Thread;
use std::comm::{channel, Sender, Receiver};
use std::vec::Vec;
//use std::sync::TaskPool;

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
#[deriving(Copy,Clone)]
pub struct SequentialScheduler;

/// Scheduler that spawns a new OS thread for each scheduled function
///
/// Gives maximum parallelism, but has high overhead.
#[deriving(Copy,Clone)]
pub struct SpawningScheduler;

/// Scheduler that queues all scheduled functions to allow for inspection and later running them in a controlled manner, e.g. in tests
#[deriving(Clone)]
pub struct TestScheduler
{
    scheduler : Sender<Thunk>,
    scheduled : Arc<Mutex<Receiver<Thunk>>>,
    scheduled_count : Arc<AtomicUint>
}

impl Send for SequentialScheduler {}
impl Send for SpawningScheduler {}
impl Send for TestScheduler {}

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
            scheduled_count : Arc::new(AtomicUint::new(0u))
        }
    }

    /// Current count of functions that has been scheduled since creation or last call to `run_queued`
    pub fn queued_count(&self) -> uint
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
        while let Ok(t) = self.scheduled.lock().try_recv()
        {
            v.push(t);
        }       
        self.scheduled_count.store(0u, Ordering::Relaxed);
        for t in v.into_iter()
        {
            t.invoke(());
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
        Thread::spawn(f).detach()
    }
}

impl Scheduler for TestScheduler
{
    /// Queue the function to be run later when `run_queued` is called
    fn schedule<F>(&self, f : F) where F : Send + FnOnce()
    {
        self.scheduler.send(Thunk::new(f));
        self.scheduled_count.fetch_add(1u, Ordering::Relaxed);
    }
}

/*
// Would have been nice, but unfortunately TaskPool does not implement Clone (but it should...)
impl Scheduler for TaskPool
{
    fn schedule<F>(&self, f : F) where F : Send + FnOnce()
    {
        self.execute(f)
    }
}
*/
