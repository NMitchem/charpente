//! Production IO: the one sanctioned boundary where ambient nondeterminism
//! enters the process (DESIGN.md §1). Everything the deny-list forbids
//! elsewhere is legal in this module and nowhere else — hence the module-wide
//! allow. Unix-only for now; Windows arrives when someone needs it.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::time::Duration;

use charpente_core::io::{
    Clock, Conn, Disk, Entropy, File, Instant, Listener, Net, ObjectDone, ObjectOp, ObjectStore,
};

// ---------------------------------------------------------------- Clock

/// Monotonic time anchored at construction (normally: process start).
pub struct ProdClock {
    origin: std::time::Instant,
}

impl ProdClock {
    #[allow(clippy::new_without_default)]
    pub fn new() -> ProdClock {
        ProdClock {
            origin: std::time::Instant::now(),
        }
    }
}

impl Clock for ProdClock {
    fn now(&self) -> Instant {
        let nanos = u64::try_from(self.origin.elapsed().as_nanos())
            .expect("process uptime exceeds u64 nanos");
        Instant::from_nanos(nanos)
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

// ---------------------------------------------------------------- Entropy

/// OS entropy from /dev/urandom, opened once.
pub struct ProdEntropy {
    urandom: std::fs::File,
}

impl ProdEntropy {
    pub fn new() -> io::Result<ProdEntropy> {
        Ok(ProdEntropy {
            urandom: std::fs::File::open("/dev/urandom")?,
        })
    }
}

impl Entropy for ProdEntropy {
    fn fill(&self, buf: &mut [u8]) {
        (&self.urandom)
            .read_exact(buf)
            .expect("/dev/urandom read failed — the OS entropy source is broken");
    }
}

// ---------------------------------------------------------------- Disk

/// Files under a root directory. The trait's flat names may contain `/`,
/// which maps to subdirectories created on demand.
pub struct ProdDisk {
    root: PathBuf,
}

impl ProdDisk {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<ProdDisk> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(ProdDisk { root })
    }

    fn resolve(&self, path: &str) -> io::Result<PathBuf> {
        assert!(
            !path.is_empty()
                && !path.starts_with('/')
                && !path
                    .split('/')
                    .any(|c| c == "." || c == ".." || c.is_empty()),
            "Disk paths are relative flat names like \"wal/000001.log\" — got \"{path}\""
        );
        Ok(self.root.join(path))
    }
}

impl Disk for ProdDisk {
    fn open(&self, path: &str) -> io::Result<Box<dyn File>> {
        let full = self.resolve(path)?;
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(full)?;
        Ok(Box::new(ProdFile { file }))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        std::fs::remove_file(self.resolve(path)?)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    stack.push(entry.path());
                } else {
                    let name = entry
                        .path()
                        .strip_prefix(&self.root)
                        .expect("walked entries live under the root")
                        .to_string_lossy()
                        .into_owned();
                    if name.starts_with(prefix) {
                        names.push(name);
                    }
                }
            }
        }
        names.sort(); // callers must never see filesystem enumeration order
        Ok(names)
    }

    fn sync_dir(&self, prefix: &str) -> io::Result<()> {
        // Prefixes are flat names ("wal/", or "" for the root); fsync the
        // directory that holds those entries. Same fsync-vs-F_FULLFSYNC
        // caveat as ProdFile::sync — revisit both together.
        let dir = match prefix.rfind('/') {
            Some(i) => self.root.join(&prefix[..i]),
            None => self.root.clone(),
        };
        std::fs::create_dir_all(&dir)?;
        std::fs::File::open(&dir)?.sync_all()
    }
}

struct ProdFile {
    file: std::fs::File,
}

impl File for ProdFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        std::os::unix::fs::FileExt::read_exact_at(&self.file, out, offset)
    }

    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        std::os::unix::fs::FileExt::write_all_at(&self.file, data, offset)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync(&self) -> io::Result<()> {
        // sync_all: on macOS this is F_FULLFSYNC — plain fsync there does not
        // reach the platter and the durability contract would be a lie.
        self.file.sync_all()
    }
}

// ---------------------------------------------------------------- Net

/// TCP with the trait's message framing: every message travels as
/// [len: u32 le][bytes]. Sockets are non-blocking to match the poll-shaped
/// trait; send spins briefly on a full kernel buffer.
pub struct ProdNet;

impl Net for ProdNet {
    fn listen(&self, addr: &str) -> io::Result<Box<dyn Listener>> {
        let listener = std::net::TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Box::new(ProdListener { listener }))
    }

    fn connect(&self, addr: &str) -> io::Result<Box<dyn Conn>> {
        let stream = std::net::TcpStream::connect(addr)?;
        ProdConn::new(stream).map(|c| Box::new(c) as Box<dyn Conn>)
    }
}

struct ProdListener {
    listener: std::net::TcpListener,
}

impl Listener for ProdListener {
    fn accept(&self) -> io::Result<Option<Box<dyn Conn>>> {
        match self.listener.accept() {
            Ok((stream, _)) => ProdConn::new(stream).map(|c| Some(Box::new(c) as Box<dyn Conn>)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn local_addr(&self) -> String {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
            .to_string()
    }
}

struct ProdConn {
    stream: std::net::TcpStream,
    /// Bytes read off the stream but not yet returned as a whole message.
    inbox: RefCell<Vec<u8>>,
    /// Set once the stream reports EOF.
    peer_closed: std::cell::Cell<bool>,
}

impl ProdConn {
    fn new(stream: std::net::TcpStream) -> io::Result<ProdConn> {
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        Ok(ProdConn {
            stream,
            inbox: RefCell::new(Vec::new()),
            peer_closed: std::cell::Cell::new(false),
        })
    }
}

impl Conn for ProdConn {
    fn send(&self, data: &[u8]) -> io::Result<()> {
        let mut frame = Vec::with_capacity(4 + data.len());
        frame.extend_from_slice(
            &(u32::try_from(data.len()).expect("message exceeds u32 length")).to_le_bytes(),
        );
        frame.extend_from_slice(data);
        let mut written = 0;
        while written < frame.len() {
            match (&self.stream).write(&frame[written..]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => std::thread::yield_now(),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn recv(&self, out: &mut [u8]) -> io::Result<Option<usize>> {
        let mut inbox = self.inbox.borrow_mut();
        // Drain whatever the socket has, without blocking.
        let mut chunk = [0u8; 16 * 1024];
        while !self.peer_closed.get() {
            match (&self.stream).read(&mut chunk) {
                Ok(0) => self.peer_closed.set(true),
                Ok(n) => inbox.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        if inbox.len() < 4 {
            return Ok(if self.peer_closed.get() && inbox.is_empty() {
                Some(0)
            } else {
                None
            });
        }
        let len = u32::from_le_bytes(inbox[0..4].try_into().expect("sliced 4 bytes")) as usize;
        if inbox.len() < 4 + len {
            return Ok(None); // message still in flight
        }
        assert!(
            out.len() >= len,
            "recv buffer ({}) smaller than message ({len}) — size the buffer to the protocol's largest message",
            out.len()
        );
        out[..len].copy_from_slice(&inbox[4..4 + len]);
        inbox.drain(..4 + len);
        Ok(Some(len))
    }

    fn peer_addr(&self) -> String {
        self.stream
            .peer_addr()
            .map_or_else(|_| "<disconnected>".to_string(), |a| a.to_string())
    }
}

// ---------------------------------------------------------- ObjectStore

/// The suffix a PUT writes through before renaming into place. Reserved:
/// a caller key ending in it would be indistinguishable from another key's
/// crash residue, so `start` refuses one rather than letting the two
/// namespaces overlap.
const TMP_SUFFIX: &str = ".tmp";

/// Objects as files under a root directory — the first non-simulator
/// `ObjectStore`, and the one v1 ships (archive spec §1 ruling 4: no S3
/// client yet). Keys map to paths, `/` to subdirectories, exactly as
/// `ProdDisk` maps its flat names.
///
/// **Completions are immediate**: `start` records the op and does no IO;
/// `poll` does the whole operation and always returns `Some` on its first
/// call. Spec §4b's 10,000-poll bound therefore never counts past 1 against
/// this store. That is a property of THIS impl, not of the trait — a store
/// with real network latency will need the bound to become a deadline.
///
/// **How the atomic-PUT contract is honoured, and what it costs here.** The
/// trait says a PUT is atomic and that there is "no `sync_dir` analogue:
/// there is no directory entry to persist". The first is the contract; the
/// second is true of a real object store and FALSE of a filesystem, which
/// has a directory entry and nothing else. `rename(2)` is atomic against a
/// process dying, but neither the renamed file's data nor the new directory
/// entry is durable against a machine crash until each is fsynced. So a PUT
/// here is four steps and all four are load-bearing: write `<key>.tmp`,
/// fsync it, rename it onto `<key>`, fsync the parent directory. Dropping
/// step 2 lets a crash resurrect the key holding zeros; dropping step 4 lets
/// a crash lose an acked object outright.
///
/// Not covered, deliberately: the ancestor directories a PUT creates on
/// first write are not themselves fsynced into their parents, so a crash in
/// that window can lose a newly created generation prefix. It degrades
/// exactly like spec §7's "PUT lands but the ack is lost" row — the retry
/// re-creates it — and closing it would mean fsyncing the whole chain on
/// every PUT.
pub struct FsObjectStore {
    root: PathBuf,
    /// One operation in flight at a time (the trait's own words). `RefCell`,
    /// not `Mutex`: `ObjectStore` requires only `Send`, and `RefCell<T>` is
    /// `Send` whenever `T` is.
    in_flight: RefCell<Option<ObjectOp>>,
}

impl FsObjectStore {
    /// The bucket root. Not created here — a store pointed at an absent
    /// directory answers `NotFound` for a `Get` and an empty `List`, which
    /// is what an empty bucket looks like; a PUT creates what it needs.
    pub fn new(root: impl Into<PathBuf>) -> FsObjectStore {
        FsObjectStore {
            root: root.into(),
            in_flight: RefCell::new(None),
        }
    }

    /// Keys are relative flat names like `"bucket/00…01/segments/00…07.log"`.
    /// Asserted, not error-returned: a malformed key is a caller bug, the
    /// same judgement `ProdDisk::resolve` and `ObjectStore::start`'s
    /// in-flight check already make.
    fn resolve(&self, key: &str) -> PathBuf {
        assert!(
            !key.is_empty()
                && !key.starts_with('/')
                && !key
                    .split('/')
                    .any(|c| c == "." || c == ".." || c.is_empty()),
            "ObjectStore keys are relative flat names like \"bucket/gen/segments/1.log\" — got \"{key}\""
        );
        assert!(
            !key.ends_with(TMP_SUFFIX),
            "ObjectStore key \"{key}\" ends in \"{TMP_SUFFIX}\", which FsObjectStore reserves \
             for the temp file every atomic PUT renames from — pick another key"
        );
        self.root.join(key)
    }

    fn put(&self, key: &str, data: &[u8]) -> io::Result<()> {
        let path = self.resolve(key);
        let parent = path
            .parent()
            .expect("resolve joins onto the root, so there is always a parent");
        std::fs::create_dir_all(parent)?;
        let tmp = path.with_file_name(format!(
            "{}{TMP_SUFFIX}",
            path.file_name()
                .expect("resolve rejects empty components")
                .to_string_lossy()
        ));
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        (&file).write_all(data)?;
        // Before the rename: the name must never point at bytes that are not
        // on the platter yet.
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, &path)?;
        // After the rename: the directory entry that makes the object exist
        // is itself a write, and an unsynced one loses an acked PUT.
        std::fs::File::open(parent)?.sync_all()
    }

    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        let path = self.resolve(key);
        let mut file = match std::fs::OpenOptions::new().read(true).open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no object at key \"{key}\""),
                ));
            }
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        file.read_to_end(&mut out)?;
        Ok(out)
    }

    /// Every key under `prefix`, sorted — never filesystem enumeration
    /// order, the same rule `Disk::list` follows. A missing root is an empty
    /// bucket, not an error. Temp files a crashed PUT left behind are not
    /// objects and are never listed.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match std::fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for entry in entries {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    stack.push(entry.path());
                    continue;
                }
                let key = entry
                    .path()
                    .strip_prefix(&self.root)
                    .expect("walked entries live under the root")
                    .to_string_lossy()
                    .into_owned();
                if !key.ends_with(TMP_SUFFIX) && key.starts_with(prefix) {
                    keys.push(key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        match std::fs::remove_file(self.resolve(key)) {
            // Deleting what is not there is not a failure — the archive's
            // retry paths and `SimObjectStore` both rely on it.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }
}

impl ObjectStore for FsObjectStore {
    fn start(&self, op: ObjectOp) -> io::Result<()> {
        // Key shape is validated HERE as well as at use: `start` is where
        // the caller's bug is, and a `List` never resolves a path at all.
        match &op {
            ObjectOp::Put { key, .. } | ObjectOp::Get { key } | ObjectOp::Delete { key } => {
                let _ = self.resolve(key);
            }
            ObjectOp::List { .. } => {}
        }
        assert!(
            self.in_flight.borrow().is_none(),
            "ObjectStore::start called with an operation already in flight — \
             poll to completion before starting another (archive spec §1)"
        );
        *self.in_flight.borrow_mut() = Some(op);
        Ok(())
    }

    fn poll(&self) -> Option<io::Result<ObjectDone>> {
        let op = self.in_flight.borrow_mut().take()?;
        Some(match op {
            ObjectOp::Put { key, data } => self
                .put(&key, &data)
                .map(|()| ObjectDone::Put)
                .map_err(|e| named(&e, "PUT", &key)),
            ObjectOp::Get { key } => self
                .get(&key)
                .map(ObjectDone::Get)
                .map_err(|e| named(&e, "GET", &key)),
            ObjectOp::List { prefix } => self
                .list(&prefix)
                .map(ObjectDone::List)
                .map_err(|e| named(&e, "LIST", &prefix)),
            ObjectOp::Delete { key } => self
                .delete(&key)
                .map(|()| ObjectDone::Delete)
                .map_err(|e| named(&e, "DELETE", &key)),
        })
    }
}

/// Hard rule 5 at the IO boundary: an `EACCES` on its own names neither the
/// operation nor the key an operator has to go and look at. `NotFound` from
/// `get` is already phrased for the caller and passes through unchanged.
fn named(e: &io::Error, op: &str, key: &str) -> io::Error {
    if e.kind() == io::ErrorKind::NotFound && op == "GET" {
        return io::Error::new(e.kind(), e.to_string());
    }
    io::Error::new(e.kind(), format!("object {op} \"{key}\" failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        let mut suffix = [0u8; 8];
        ProdEntropy::new().unwrap().fill(&mut suffix);
        std::env::temp_dir().join(format!(
            "charpente-{tag}-{:016x}",
            u64::from_le_bytes(suffix)
        ))
    }

    #[test]
    fn clock_is_monotonic_and_sleeps() {
        let clock = ProdClock::new();
        let before = clock.now();
        clock.sleep(Duration::from_millis(5));
        let after = clock.now();
        assert!(after > before);
        assert!(after.duration_since(before) >= Duration::from_millis(4));
    }

    #[test]
    fn entropy_is_not_constant() {
        let entropy = ProdEntropy::new().unwrap();
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        entropy.fill(&mut a);
        entropy.fill(&mut b);
        assert_ne!(
            a, b,
            "two 16-byte draws colliding means the entropy source is broken"
        );
    }

    #[test]
    fn disk_roundtrip_through_the_trait() {
        let root = temp_root("disk");
        let disk = ProdDisk::new(&root).unwrap();

        let file = disk.open("wal/000001.log").unwrap();
        file.write_at(0, b"hello").unwrap();
        file.write_at(8, b"world").unwrap(); // gap reads as zeros
        file.sync().unwrap();
        assert_eq!(file.len().unwrap(), 13);

        let mut out = [0u8; 13];
        file.read_at(0, &mut out).unwrap();
        assert_eq!(&out, b"hello\0\0\0world");

        file.set_len(5).unwrap();
        assert_eq!(file.len().unwrap(), 5);

        disk.open("wal/000002.log").unwrap();
        disk.open("other.dat").unwrap();
        assert_eq!(
            disk.list("wal/").unwrap(),
            vec!["wal/000001.log", "wal/000002.log"]
        );

        disk.remove("other.dat").unwrap();
        assert!(disk.list("other").unwrap().is_empty());

        std::fs::remove_dir_all(root).unwrap();
    }

    /// Polls until `step` yields, panicking past the deadline.
    fn poll_until<T>(what: &str, mut step: impl FnMut() -> Option<T>) -> T {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(value) = step() {
                return value;
            }
            assert!(std::time::Instant::now() < deadline, "{what} timed out");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn net_echo_over_loopback() {
        let net = ProdNet;
        let listener = net.listen("127.0.0.1:0").unwrap();
        let client = net.connect(&listener.local_addr()).unwrap();
        let server = poll_until("accept", || listener.accept().unwrap());

        client.send(b"ping").unwrap();
        let mut buf = [0u8; 64];
        let n = poll_until("server recv", || server.recv(&mut buf).unwrap());
        assert_eq!(&buf[..n], b"ping");

        server.send(b"pong back").unwrap();
        let n = poll_until("client recv", || client.recv(&mut buf).unwrap());
        assert_eq!(&buf[..n], b"pong back");

        // Two messages sent back-to-back arrive as two framed messages.
        client.send(b"one").unwrap();
        client.send(b"two").unwrap();
        let n = poll_until("first framed recv", || server.recv(&mut buf).unwrap());
        assert_eq!(&buf[..n], b"one");
        let n = poll_until("second framed recv", || server.recv(&mut buf).unwrap());
        assert_eq!(&buf[..n], b"two");

        // Close is observable as a zero-length recv.
        drop(client);
        let n = poll_until("close notification", || {
            match server.recv(&mut buf).unwrap() {
                Some(0) => Some(0),
                _ => None,
            }
        });
        assert_eq!(n, 0);
    }
}
