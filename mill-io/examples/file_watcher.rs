use mill_io::{error::Result, EventHandler, EventLoop};
use mio::{event::Event, Token};
use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};

#[cfg(target_os = "linux")]
use mio::unix::SourceFd;
#[cfg(target_os = "linux")]
use std::{ffi::OsStr, os::unix::io::AsRawFd};

#[cfg(target_os = "linux")]
use mio::Interest;

static EVENT_LOOP: OnceLock<EventLoop> = OnceLock::new();
fn event_loop() -> &'static EventLoop {
    EVENT_LOOP.get_or_init(EventLoop::default)
}

#[derive(Debug, Clone)]
pub enum FileEvent {
    Created(PathBuf),
    Modified(PathBuf),
    Deleted(PathBuf),
}

pub struct FileWatcher {
    watches: Arc<Mutex<HashMap<Token, PathBuf>>>,
    #[cfg(target_os = "linux")]
    inotify: Arc<Mutex<inotify::Inotify>>,
}

impl FileWatcher {
    pub fn new() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            let inotify = inotify::Inotify::init()?;
            Ok(FileWatcher {
                watches: Arc::new(Mutex::new(HashMap::new())),
                inotify: Arc::new(Mutex::new(inotify)),
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            Ok(FileWatcher {
                watches: Arc::new(Mutex::new(HashMap::new())),
            })
        }
    }

    pub fn watch_path<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        let token = event_loop().alloc_token();

        println!("[INFO] Watching path={:?}, token={:?}", path, token);

        #[cfg(target_os = "linux")]
        {
            let inotify = self.inotify.lock().unwrap();
            let _ = inotify.watches().add(
                &path,
                inotify::WatchMask::MODIFY
                    | inotify::WatchMask::CREATE
                    | inotify::WatchMask::DELETE
                    | inotify::WatchMask::MOVED_TO
                    | inotify::WatchMask::MOVED_FROM,
            )?;
            let source_fd = inotify.as_raw_fd();
            let mut source_fd = SourceFd(&source_fd);
            event_loop().register(
                &mut source_fd,
                token,
                Interest::READABLE,
                FileEventHandler {
                    token,
                    path: path.clone(),
                    inotify: self.inotify.clone(),
                },
            )?;

            self.watches.lock().unwrap().insert(token, path);
        }

        #[cfg(not(target_os = "linux"))]
        {
            // For non-Linux platforms, we'll use a simple polling approach
            // In a real implementation, you'd use platform-specific APIs
            println!("File watching on this platform requires platform-specific implementation");
            self.watches.lock().unwrap().insert(token, path);
        }

        Ok(())
    }

    pub fn list_watches(&self) -> Vec<PathBuf> {
        self.watches.lock().unwrap().values().cloned().collect()
    }
}

pub struct FileEventHandler {
    token: Token,
    path: PathBuf,
    #[cfg(target_os = "linux")]
    inotify: Arc<Mutex<inotify::Inotify>>,
}
impl EventHandler for FileEventHandler {
    fn handle_event(&self, event: &Event) {
        if event.token() != self.token {
            return;
        }

        if event.is_readable() {
            #[cfg(target_os = "linux")]
            {
                let mut inotify = self.inotify.lock().unwrap();
                let mut buffer = [0; 4096];

                match inotify.read_events(&mut buffer) {
                    Ok(events) => {
                        for event in events {
                            self.process_file_event(event);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading inotify events: {}", e);
                    }
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                println!("File event detected for path: {:?}", self.path);
            }
        }
    }
}

impl FileEventHandler {
    #[cfg(target_os = "linux")]
    fn process_file_event(&self, event: inotify::Event<&OsStr>) {
        let file_path = if let Some(name) = event.name {
            self.path.join(name)
        } else {
            self.path.clone()
        };

        let file_event = if event.mask.contains(inotify::EventMask::CREATE)
            || event.mask.contains(inotify::EventMask::MOVED_TO)
        {
            FileEvent::Created(file_path)
        } else if event.mask.contains(inotify::EventMask::MODIFY) {
            FileEvent::Modified(file_path)
        } else if event.mask.contains(inotify::EventMask::DELETE)
            || event.mask.contains(inotify::EventMask::MOVED_FROM)
        {
            FileEvent::Deleted(file_path)
        } else {
            return;
        };

        self.handle_file_event(file_event);
    }

    #[cfg(target_os = "linux")]
    fn handle_file_event(&self, event: FileEvent) {
        match event {
            FileEvent::Created(path) => {
                println!("[INFO] File created: {:?}", path);
            }
            FileEvent::Modified(path) => {
                println!("[INFO] File modified: {:?}", path);
            }
            FileEvent::Deleted(path) => {
                println!("[INFO] File deleted: {:?}", path);
            }
        }
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let paths_to_watch: Vec<PathBuf> = if args.len() > 1 {
        args[1..].iter().map(PathBuf::from).collect()
    } else {
        vec![PathBuf::from(".")]
    };

    for path in &paths_to_watch {
        if !path.exists() {
            eprintln!(
                "Error: Path {:?} does not exist",
                path.canonicalize().unwrap()
            );
            eprintln!(
                "Error: Path {:?} does not exist",
                path.canonicalize().unwrap()
            );
            return Ok(());
        }
    }

    let watcher = FileWatcher::new()?;

    println!("[INFO] Starting file watcher...");
    println!("[INFO] Press Ctrl+C to stop\n");

    for path in &paths_to_watch {
        match watcher.watch_path(path) {
            Ok(()) => println!(
                "[INFO] Watching path={:#?}",
                path.canonicalize()
                    .expect("[ERROR] Could not get the path canonicalized")
            ),
            Err(e) => {
                eprintln!("[ERROR] Failed to watch {:?}: {}", path, e);
                #[cfg(not(target_os = "linux"))]
                eprintln!("[ERROR] Full file watching requires Linux with inotify support");
            }
        }
    }

    println!("[INFO] Active watches: {:?}", watcher.list_watches());

    #[cfg(not(target_os = "linux"))]
    {
        println!("[WARNING] Running on non-Linux platform.");
        println!("[WARNING[ File watching functionality is limited without inotify.");
        println!("[WARNING[ Consider running on Linux for full functionality.");
    }

    match event_loop().run() {
        Ok(()) => println!("[INFO] File watcher stopped cleanly"),
        Err(e) => eprintln!("[ERROR] File watcher error: {}", e),
    }

    Ok(())
}
