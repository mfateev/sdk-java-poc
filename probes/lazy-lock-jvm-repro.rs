use std::{
    future::{Future, poll_fn},
    pin::pin,
    ptr,
    sync::{LazyLock, Mutex},
    task::{Context, Poll, Waker},
};

struct Worker {
    subsystems: LazyLock<Subsystems, Box<dyn FnOnce() -> Subsystems + Send>>,
}
struct Subsystems {
    workflows: Option<Workflow>,
}
struct Workflow {
    activation: Mutex<u64>,
}

async fn yield_once() {
    let mut yielded = false;
    poll_fn(move |_| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

impl Worker {
    async fn poll(&self) -> (&Mutex<u64>, u64) {
        let lock = &self.subsystems.workflows.as_ref().unwrap().activation;
        yield_once().await;
        let mut guard = lock
            .try_lock()
            .expect("mutex held after earlier call finished");
        let prior = *guard;
        *guard = prior + 1;
        (lock, prior)
    }
}

async fn run() {
    let worker = Worker {
        subsystems: LazyLock::new(Box::new(|| Subsystems {
            workflows: Some(Workflow {
                activation: Mutex::new(11),
            }),
        })),
    };
    let a = worker.poll().await;
    let b = worker.poll().await;
    println!(
        "lock addresses: {:#x} -> {:#x}; values: {} -> {}",
        ptr::from_ref(a.0).addr(),
        ptr::from_ref(b.0).addr(),
        a.1,
        b.1
    );
    assert!(
        ptr::eq(a.0, b.0),
        "two live borrows of the same mutex must alias"
    );
    assert_eq!(b.1, 12);
}

fn main() {
    let mut future = pin!(run());
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..10 {
        if future.as_mut().poll(&mut cx).is_ready() {
            return;
        }
    }
    panic!("future did not finish");
}
