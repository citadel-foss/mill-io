use parking_lot::Mutex;
use std::any::Any;
#[cfg(feature = "unstable-mpmc")]
use std::sync::mpmc as channel;
#[cfg(not(feature = "unstable-mpmc"))]
use std::sync::mpsc as channel;
use std::sync::Arc;

#[derive(Clone)]
pub struct ObjectPool<T> {
    sender: channel::Sender<T>,
    receiver: Arc<Mutex<channel::Receiver<T>>>,
    create_fn: Arc<dyn Fn() -> T + Send + Sync>,
    /// For `Vec<u8>` pools: the length `create_fn` gives, restored on every `acquire`.
    buffer_len: Option<usize>,
}

impl<T: Send + 'static> ObjectPool<T> {
    pub fn new<F>(initial_size: usize, create_fn: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        let (sender, receiver) = channel::channel();

        let first = create_fn();
        let buffer_len = (&first as &dyn Any).downcast_ref::<Vec<u8>>().map(Vec::len);
        if initial_size > 0 {
            sender.send(first).expect("Failed to initialize ObjectPool");
        }
        for _ in 1..initial_size {
            sender
                .send(create_fn())
                .expect("Failed to initialize ObjectPool");
        }

        Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            create_fn: Arc::new(create_fn),
            buffer_len,
        }
    }

    /// Takes a free object, or makes one. A `Vec<u8>` comes back at its original length
    /// with the bytes its last user left; only bytes added back to restore it are zero.
    pub fn acquire(&self) -> PooledObject<T> {
        let mut object = {
            let receiver = self.receiver.lock();
            match receiver.try_recv() {
                Ok(obj) => obj,
                Err(channel::TryRecvError::Empty) => (self.create_fn)(),
                Err(channel::TryRecvError::Disconnected) => {
                    panic!("ObjectPool sender disconnected!");
                }
            }
        };

        if let (Some(len), Some(vec)) = (
            self.buffer_len,
            (&mut object as &mut dyn Any).downcast_mut::<Vec<u8>>(),
        ) {
            vec.resize(len, 0);
        }

        PooledObject {
            object: Some(object),
            pool_sender: self.sender.clone(),
        }
    }
}

pub struct PooledObject<T> {
    object: Option<T>,
    pool_sender: channel::Sender<T>,
}

impl<T> std::convert::AsMut<T> for PooledObject<T> {
    fn as_mut(&mut self) -> &mut T {
        self.object.as_mut().expect("PooledObject is empty")
    }
}

impl<T> std::convert::AsRef<T> for PooledObject<T> {
    fn as_ref(&self) -> &T {
        self.object.as_ref().expect("PooledObject is empty")
    }
}

impl<T> Drop for PooledObject<T> {
    fn drop(&mut self) {
        if let Some(object) = self.object.take() {
            let _ = self.pool_sender.send(object);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_keeps_the_length_create_fn_gives() {
        for len in [512, 8192, 65536] {
            let pool = ObjectPool::new(2, move || vec![0u8; len]);
            for _ in 0..4 {
                let mut buffer = pool.acquire();
                assert_eq!(buffer.as_ref().len(), len);
                buffer.as_mut().truncate(3);
            }
        }
        let empty_start = ObjectPool::new(0, || vec![0u8; 100]);
        assert_eq!(empty_start.acquire().as_ref().len(), 100);
    }

    #[test]
    fn acquire_zeroes_only_the_bytes_it_adds_back() {
        let pool = ObjectPool::new(1, || vec![0u8; 8]);
        pool.acquire().as_mut().fill(7);
        assert_eq!(pool.acquire().as_ref(), &[7; 8]);

        let mut buffer = pool.acquire();
        buffer.as_mut().truncate(3);
        drop(buffer);
        assert_eq!(pool.acquire().as_ref(), &[7, 7, 7, 0, 0, 0, 0, 0]);
    }
}
