use std::sync::{Condvar, Mutex};

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SignalStatus {
    Ready,
    Waiting,
    Terminated,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum SignalReturnStatus {
    Ready,
    Terminated,
}

pub struct Signal {
    condvar: Condvar,
    status: Mutex<SignalStatus>,
}

impl Signal {
    pub fn new() -> Self {
        Self {
            condvar: Condvar::new(),
            status: Mutex::new(SignalStatus::Waiting),
        }
    }

    pub fn wait(&self) -> SignalReturnStatus {
        let mut status = *self.status.lock().unwrap();
        while let SignalStatus::Waiting = status {
            status = *self.condvar.wait(self.status.lock().unwrap()).unwrap();
        }
        // wait completed

        // read the status, return, and reset the status to Waiting
        let status = *self.status.lock().unwrap();

        *self.status.lock().unwrap() = SignalStatus::Waiting;

        match status {
            SignalStatus::Ready => SignalReturnStatus::Ready,
            SignalStatus::Terminated => SignalReturnStatus::Terminated,
            SignalStatus::Waiting => unreachable!(),
        }
    }

    pub fn set(&self) {
        let mut status = self.status.lock().unwrap();
        *status = SignalStatus::Ready;
        self.condvar.notify_all();
    }

    pub fn kill(&self) {
        let mut status = self.status.lock().unwrap();
        *status = SignalStatus::Terminated;
        self.condvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_signal() {
        let signal = Arc::new(Signal::new());
        let signal_clone1 = Arc::clone(&signal);
        let signal_clone2 = Arc::clone(&signal);

        let res = Arc::new(Mutex::new(0));
        let res_clone = Arc::clone(&res);

        // Spawn a thread that will wait for the signal
        let handle = thread::spawn(move || {
            signal_clone1.wait();
            *res_clone.lock().unwrap() += 1;
        });

        let setter = thread::spawn(move || {
            // sleep for 0.1s
            thread::sleep(Duration::from_millis(100));
            // Set the signal to wake up the waiting thread
            signal_clone2.set();
        });

        setter.join().unwrap();

        // sleep for 0.1s
        thread::sleep(Duration::from_millis(100));
        assert_eq!(*res.lock().unwrap(), 1);

        // Wait for the thread to finish
        handle.join().unwrap();

        // Ensure the signal was set to waiting.
        assert_eq!(*signal.status.lock().unwrap(), SignalStatus::Waiting);
    }

    #[test]
    fn test_signal_multiple_setters() {
        let signal = Arc::new(Signal::new());
        let rounds = 5;
        let res = Arc::new(Mutex::new(0));

        // Spawn multiple threads that will set the signal
        let mut setter_handles = vec![];
        for _ in 0..rounds {
            let signal_clone = Arc::clone(&signal);
            let handle = thread::spawn(move || {
                signal_clone.set();
            });
            setter_handles.push(handle);
        }

        // Spawn a thread that will wait for the signal
        let waiter_handle = {
            let signal_clone = Arc::clone(&signal);
            let res_clone = Arc::clone(&res);

            thread::spawn(move || {
                thread::sleep(Duration::from_millis(100));
                signal_clone.wait();
                *res_clone.lock().unwrap() += 1;

                assert_eq!(*signal_clone.status.lock().unwrap(), SignalStatus::Waiting);

                // Reset the signal after consumption
                let mut status = signal_clone.status.lock().unwrap();
                *status = SignalStatus::Waiting;
            })
        };

        // Wait for all setter threads to finish
        for handle in setter_handles {
            handle.join().unwrap();
        }

        // Wait for the waiter thread to finish
        waiter_handle.join().unwrap();

        // Verify that the waiter only waited for 1 time although 5 sets are done
        assert_eq!(*res.lock().unwrap(), 1);

        // Ensure the signal was reset correctly
        assert_eq!(*signal.status.lock().unwrap(), SignalStatus::Waiting);
    }
}
