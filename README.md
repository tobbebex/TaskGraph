[![Build Status][status]](https://travis-ci.org/tobbebex/TaskGraph)

TaskGraph
=========

Create task graphs in rust, where tasks with dependencies on each other are run in parallel.

###[API Documentation](http://tobbebex.github.io/TaskGraph/)

[status]: https://travis-ci.org/tobbebex/TaskGraph.svg?branch=master

###Example usage

Let's start with a long running function that we want to parallelize:

    use task_graph::task;
    use task_graph::task::Join;
    use task_graph::scheduler;
    use std::sync::{Arc,Mutex,TaskPool};
    use std::io::timer::sleep;
    use std::time::duration::Duration;
    use std::os::num_cpus;
    
    fn long_running(name : char, length : uint)
    {
    	for _ in range(0, length)
    	{
    		println!("{}", name);
    		sleep(Duration::seconds(1));
    	}
    }


In order to use TaskGraph, we need to create a scheduler. Most of the time, this is what you want:

    let pool = Arc::new(Mutex::new(TaskPool::new(num_cpus())));

Now lets create a task that runs in parallel with some other work:

    task::Task::from_fn(&pool, || { long_running('A', 4); } );
    long_running('B',4);

Running this on a machine with 2 or more cores gives this result:

    A
    B
    A
    B
    B
    A
    B
    A

---

Tasks can be set up as continuations on other tasks with `this`:

    let a = task::Task::from_fn(&pool, || { long_running('A', 4); return 42i; } );
    let b = a.then(&pool, |res| { println!("B got {}", res); } );
    b.into_future().get();
    
Here we had to throw in a `into'_future` in order to wait for the last task to finish before we ended the application. Most of the time you shouldn't use futures but instead just chain the result to another task with `then`. Running the above yields:

    A
    A
    A
    A
    B got 42

---

Not only are tasks running in parallel with the main thread, but also with each other. To make a continuation that depends on two tasks, use `join`:

    let a = task::Task::from_fn(&pool, || { long_running('A', 4); return 42i; } );
    let b = task::Task::from_fn(&pool, || { long_running('B', 2); return 7i; } );
    let c = a.join(b).then(&pool, |(res1,res2)| {  println!("C got {} and {}", res1, res2); } );
    c.into_future().get();

Which yields:

    A
    B
    B
    A
    A
    A
    C got 42 and 7

---

You can have multiple continuations to the same task as well, if you clone it first:

    let a = task::Task::from_fn(&pool, || { long_running('A', 4); return 42i; } );
    let b = a.clone().then(&pool, |res| { println!("B got {}", res); long_running('B', 4); } );
    let c = a.then(&pool, |res| { println!("C got {}", res); long_running('C', 4); } );
    b.into_future().get();
    c.into_future().get();

```
A
A
A
A
B got 42
B
C got 42
C
C
B
B
C
B
C
```
---

With the function `join_all` you can join on more than two tasks at a time (but they have to be the same type):

    let a = task::Task::from_fn(&pool, || { long_running('A', 4); return 42i; } );
    let b = task::Task::from_fn(&pool, || { long_running('B', 2); return 7i; } );
    let c = task::Task::from_fn(&pool, || { long_running('C', 1); return -1i; } );
    let d = vec![a,b,c].join_all().then(&pool, |ress| {  println!("D got sum {}", ress.iter().fold(0, |a, &b| a + b)); } );
    d.into_future().get();

```
A
B
C
A
B
A
A
D got sum 48
```
---

There is also a `join_any` that doesnt wait for all the tasks to finish, but runs as soon as one of the tasks are finished:

    let a = task::Task::from_fn(&pool, || { long_running('A', 4); return 42i; } );
    let b = task::Task::from_fn(&pool, || { long_running('B', 2); return 7i; } );
    let c = task::Task::from_fn(&pool, || { long_running('C', 1); return -1i; } );
    let d = vec![a.clone(),b.clone(),c.clone()].join_any().then(&pool, |res| {  println!("D got {} first", res); } );

    d.into_future().get();
    vec![a,b,c].join_all().into_future().get();

Since `join_any` only waits for one of its dependencies, we had to throw in an `into_future` on a `join_all` and wait for that in order to see the rest of the tasks finishing: 

    B
    A
    C
    B
    A
    D got -1 first
    A
    A

---

Mix and match thens and joins to create any task graph you like. In this example, we also use the `SequentialScheduler` for short running tasks to minimize overhead:

    let a = task::Task::from_fn(&pool, || { long_running('A', 4); return 42i; } );
    let b = task::Task::from_fn(&pool, || { long_running('B', 2); return 7i; } );
    let c = b.clone().then(&pool, |res| { println!("C got {}", res); long_running('C', 1); return res-1i; } );
    let d = vec![a.clone(),c.clone()].join_any().then(&scheduler::SequentialScheduler, |res| { println!("D got {} first", res); return res; } );
    let e = task::Task::from_fn(&pool, || { long_running('E', 7); return 77i; } );	
    let f = d.join(e).then(&scheduler::SequentialScheduler, |(d,e)| { d + e } );
    let g = vec![a,b,c,f].join_all().then(&pool, |ress| {  println!("G got sum {}", ress.iter().fold(0, |a, &b| a + b)); } );
    g.into_future().get();

```
A
B
E
A
B
E
A
E
C got 7
C
E
A
D got 6 first
E
E
E
G got sum 138
```
