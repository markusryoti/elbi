use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, SendError, Sender},
    },
    thread::{self, JoinHandle},
};

use tracing::info;

pub struct ThreadPool {
    workers: Vec<Worker>,
    tx: Option<Sender<Job>>,
}

type Job = Box<dyn FnOnce() + Send>;

struct Worker {
    id: usize,
    handle: JoinHandle<()>,
}

impl Worker {
    fn new(id: usize, rx: Arc<Mutex<Receiver<Job>>>) -> Self {
        let handle = thread::spawn(move || {
            info!("Starting worker: {}", id);

            loop {
                let job = if let Ok(ch) = rx.lock() {
                    ch.recv().ok()
                } else {
                    None
                };

                let Some(job) = job else { break };

                info!("Worker: {} handling job", id);
                job();
            }
        });

        Worker { id, handle }
    }
}

impl ThreadPool {
    pub fn new(num_workers: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));

        let workers: Vec<Worker> = (0..num_workers)
            .map(|i| Worker::new(i, Arc::clone(&rx)))
            .collect();

        ThreadPool {
            workers,
            tx: Some(tx),
        }
    }

    pub fn enqueue(&self, job: Job) -> Result<(), SendError<Job>> {
        match &self.tx {
            Some(tx) => tx.send(job),
            None => Err(SendError(job)),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.tx.take());

        for worker in self.workers.drain(..) {
            info!("Shutting down worker {}", worker.id);

            let _ = worker.handle.join();
        }
    }
}
