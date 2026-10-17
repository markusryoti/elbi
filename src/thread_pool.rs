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
    tx: Sender<Job>,
}

type Job = Box<dyn FnOnce() + Send>;

struct Worker {
    id: usize,
    handle: JoinHandle<Job>,
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

                if let Some(job) = job {
                    info!("Worker: {} handling job", id);
                    job();
                }
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

        ThreadPool { workers, tx }
    }

    pub fn enqueue(&self, job: Job) -> Result<(), SendError<Job>> {
        self.tx.send(job)?;
        Ok(())
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for worker in self.workers.drain(..) {
            info!("Shutting down worker {}", worker.id);

            let _ = worker.handle.join().unwrap();
        }
    }
}
