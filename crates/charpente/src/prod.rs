//! Production IO: the one sanctioned boundary where ambient nondeterminism
//! enters the process (DESIGN.md §1). Everything the deny-list forbids
//! elsewhere is legal in this module and nowhere else — hence the module-wide
//! allow. Unix-only for now; Windows arrives when someone needs it.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
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

// ---------------------------------------------------------------- the filesystem seam

/// The filesystem calls `ProdDisk` and `FsObjectStore` make, one call per
/// method: production's are `std::fs`'s (`StdFs`), and a test swaps in a
/// model of what a power loss keeps (`CrashFs`, in this module's tests).
/// Every durability decision — which file and which directory is fsynced,
/// and in what order — is made above this line, so the model tests the code
/// production runs. A file is synced through the handle that wrote it: on
/// Linux a writeback error is reported to the descriptors open when it
/// happened, so a sync through a fresh descriptor could succeed over bytes
/// that were lost.
///
/// Why not `Disk`, for `FsObjectStore`: `Disk` has no rename, so a PUT
/// through it could not be atomic; its `open` creates, so a GET through it
/// would; and its names are a flat namespace whose `sync_dir` makes
/// everything under a prefix durable, so the simulator cannot express a
/// directory whose own entry was never synced — the shape both users of this
/// seam must defend against (DESIGN §1).
///
/// **What the model does not show.** It is the POSIX minimum and nothing
/// else. At a power loss every unsynced byte and every unsynced directory
/// entry is lost, deterministically and in full: never torn, never a subset
/// surviving, never reordered. So it finds a missing or misplaced sync, not
/// a bug that needs some unsynced state to survive while the rest is lost;
/// the simulator's crash collapse explores those, for the core. And its
/// kernel is honest: a failed sync leaves the state pending and a later sync
/// makes it durable, so Linux marking pages clean after an fsync error
/// (DESIGN §2's EIO residual) is not modelled — the same limit as `SimDisk`.
trait Fs: Send {
    /// `mkdir(2)`: one level, whose parent must exist.
    fn create_dir(&self, dir: &Path) -> io::Result<()>;
    /// `Path::is_dir`: `false` on any error.
    fn is_dir(&self, path: &Path) -> bool;
    /// Opens `path` for reading and writing, creating it empty if absent.
    fn open_rw(&self, path: &Path) -> io::Result<Box<dyn File>>;
    /// Creates or truncates `path` for writing.
    fn create(&self, path: &Path) -> io::Result<Box<dyn File>>;
    /// Opens `path` for reading.
    fn open(&self, path: &Path) -> io::Result<Box<dyn File>>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// fsync of the directory itself, which makes its entries durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// `(path, is a directory)` for every entry, in no particular order.
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<(PathBuf, bool)>>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
}

struct StdFs;

impl Fs for StdFs {
    fn create_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir(dir)
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn open_rw(&self, path: &Path) -> io::Result<Box<dyn File>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        Ok(Box::new(ProdFile { file }))
    }

    fn create(&self, path: &Path) -> io::Result<Box<dyn File>> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Box::new(ProdFile { file }))
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn File>> {
        let file = std::fs::File::open(path)?;
        Ok(Box::new(ProdFile { file }))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        // Same fsync-vs-F_FULLFSYNC caveat as ProdFile::sync — revisit both
        // together.
        std::fs::File::open(dir)?.sync_all()
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<(PathBuf, bool)>> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            entries.push((entry.path(), entry.file_type()?.is_dir()));
        }
        Ok(entries)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
}

/// Creates `root` when it is absent — and nothing above it. Every barrier's
/// chain ends at the root's parent, so a directory created above that would
/// hold an entry no chain ever syncs, and existing is not being durable: the
/// boot that created it may have died before anything synced it. So a root
/// whose parent is missing is refused by name, and the durability boundary
/// is the parent the operator made. `what` names the root ("data
/// directory", "bucket root"); `acked` names what rests on it.
fn create_root(fs: &dyn Fs, root: &Path, what: &str, acked: &str) -> io::Result<()> {
    match fs.create_dir(root) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let parent = parent_dir(root).expect("`/` always exists, so it is never NotFound");
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "the {what} \"{}\" cannot be created: its parent \"{}\" does not exist, and \
                     charpente never creates a directory above the {what} — {acked} rests on \
                     every directory entry above it, and charpente vouches only for the entries \
                     it creates and fsyncs. Create \"{}\" (mkdir -p \"{}\"), or point at a {what} \
                     whose parent exists",
                    root.display(),
                    parent.display(),
                    parent.display(),
                    parent.display()
                ),
            ))
        }
        Err(_) if fs.is_dir(root) => Ok(()),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!(
                "the {what} \"{}\" cannot be created: {e} — check that path, and that its parent \
                 is a directory you can write",
                root.display()
            ),
        )),
    }
}

/// `std::fs::create_dir_all` bounded by `root`: creates `dir` and whatever
/// is missing between it and `root`, never `root` itself (its creator's
/// job, `create_root`) or anything above it.
fn create_under(fs: &dyn Fs, root: &Path, dir: &Path) -> io::Result<()> {
    assert!(
        dir.starts_with(root),
        "create_under: \"{}\" is not under the root \"{}\" — every path here comes from a \
         resolve that joins onto the root",
        dir.display(),
        root.display()
    );
    if dir == root {
        return Ok(());
    }
    match fs.create_dir(dir) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) if fs.is_dir(dir) => return Ok(()),
        Err(e) => return Err(e),
    }
    let parent = dir
        .parent()
        .expect("a directory strictly under the root has a parent");
    create_under(fs, root, parent)?;
    match fs.create_dir(dir) {
        Ok(()) => Ok(()),
        Err(_) if fs.is_dir(dir) => Ok(()),
        Err(e) => Err(e),
    }
}

/// The directory holding `dir`'s own entry; `.` for a one-component
/// relative path, `None` for `/`.
fn parent_dir(dir: &Path) -> Option<PathBuf> {
    match dir.parent()? {
        p if p.as_os_str().is_empty() => Some(PathBuf::from(".")),
        p => Some(p.to_path_buf()),
    }
}

/// Where every sync chain from under `root` ends: the root's parent, which
/// holds the root's own entry — the highest directory charpente creates an
/// entry in, since it never creates above the root (`create_root`).
fn chain_end(root: &Path) -> PathBuf {
    parent_dir(root).unwrap_or_else(|| root.to_path_buf())
}

/// Fsyncs `dir` and every directory above it, up to and including `top`:
/// each holds the entry of the one below it, so a name under `dir` is
/// reachable after a power loss only once all of them are durable. The
/// whole chain on every call, never only what this process created: a
/// directory that a process which then died created, or that a call which
/// then failed created, exists without being durable, and nothing the next
/// process can see tells it from one that is. Existing is not being durable.
/// `owed` finishes the error's sentence: what waits on the chain, by whose
/// contract.
fn sync_up(fs: &dyn Fs, dir: &Path, top: &Path, owed: &str) -> io::Result<()> {
    let mut at = dir.to_path_buf();
    loop {
        sync_dir_named(fs, &at, owed)?;
        if at == top {
            return Ok(());
        }
        at = parent_dir(&at).filter(|p| *p != at).unwrap_or_else(|| {
            panic!(
                "sync_up: \"{}\" is not below \"{}\" — a sync chain climbs from a \
                 directory to one above it, and every chain here starts under the root it \
                 stops above",
                dir.display(),
                top.display()
            )
        });
    }
}

fn sync_dir_named(fs: &dyn Fs, dir: &Path, owed: &str) -> io::Result<()> {
    fs.sync_dir(dir).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "could not fsync directory \"{}\" ({e}) — {owed}; check that directory's \
                 permissions and the disk under it",
                dir.display()
            ),
        )
    })
}

// ---------------------------------------------------------------- Disk

/// What an unsynced directory costs a `Disk` name, for the error that names
/// it.
const DISK_OWED: &str = "a name is durable only once the directory holding it and every \
                         directory above it, up to the data directory's parent, have been \
                         fsynced (Disk::sync_dir's contract)";

/// Files under a root directory, the node's data directory. The trait's flat
/// names may contain `/`, which maps to subdirectories created on demand.
///
/// **Directory entries.** A name survives a power loss only once the
/// directory holding it has been fsynced, and that directory's own entry in
/// its parent, and so on up: a power loss that drops `wal`'s entry in the
/// data directory drops every segment under it, synced or not. So
/// `sync_dir(prefix)` fsyncs every directory a name starting with `prefix`
/// sits in, then every directory above them up to the data directory's
/// parent, which holds the data directory's own entry. The whole chain on
/// every barrier, not only what the barrier's own call created: a boot that
/// created a directory and then failed or died before syncing it leaves one
/// that exists without being durable, and the next boot cannot tell it from
/// one that is. `open` creates directories and syncs nothing, because
/// `Disk`'s contract makes a name durable at the barrier, not at the create.
///
/// **The data directory's parent is the boundary.** `new` creates the data
/// directory when it is absent and nothing above it: a data directory whose
/// parent does not exist is refused by name, with the fix (create the
/// parent, or point at a data directory whose parent exists). Every
/// directory the node creates is then at or below the data directory, so
/// every entry it creates lies on some barrier's chain, whichever boot
/// created it; the parent's own entry, above, is the operator's. `new`
/// syncs nothing itself — the data directory's entry is on every barrier's
/// chain, and nothing is durable before a barrier. Fsyncing a directory
/// means opening it, so the parent must also be readable by the node.
///
/// Until 2026-10-01 a barrier fsynced only the one directory holding its
/// prefix, and nothing synced a created directory into its parent. On a
/// fresh node the store's creation syncs the data directory before `wal/`
/// exists, so every write acknowledged before the first checkpoint rested
/// on a `wal` entry the next power loss dropped. `SimDisk` could not show
/// it: its names are flat, so a barrier there makes every name under the
/// prefix durable at any depth, which is the contract as written. Held by
/// the filesystem model in this module's tests (`CrashFs`): one pin per
/// sync, and a power cut after every filesystem call of a fresh node's boot
/// and first writes. Until the same day `new` also created every missing
/// directory above the data directory, whose entries no barrier reached and
/// which a boot that failed before syncing them left for the next boot to
/// trust; it now refuses that path instead.
pub struct ProdDisk {
    root: PathBuf,
    fs: Box<dyn Fs>,
}

impl ProdDisk {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<ProdDisk> {
        ProdDisk::over(root.into(), Box::new(StdFs))
    }

    /// `new` over any filesystem: the crash model, in this module's tests.
    fn over(root: PathBuf, fs: Box<dyn Fs>) -> io::Result<ProdDisk> {
        // No sync here: the new entry, in the root's parent, is on every
        // barrier's chain, and nothing is durable before a barrier.
        create_root(&*fs, &root, "data directory", "an acknowledged write")?;
        Ok(ProdDisk { root, fs })
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
            create_under(&*self.fs, &self.root, parent)?; // `sync_dir`'s to make durable
        }
        self.fs.open_rw(&full)
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.fs.remove_file(&self.resolve(path)?)
    }

    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            for (path, is_dir) in self.fs.read_dir(&dir)? {
                if is_dir {
                    stack.push(path);
                } else {
                    let name = path
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
        // Prefixes are flat names ("wal/", or "" for the root). The names
        // starting with one live in the directory holding the prefix's last
        // component and in any directory below it whose own name starts
        // with the prefix; every directory above them holds an entry on
        // their path. All of them, or a name the contract calls durable is
        // not.
        let dir = match prefix.rfind('/') {
            Some(i) => self.root.join(&prefix[..i]),
            None => self.root.clone(),
        };
        create_under(&*self.fs, &self.root, &dir)?;
        let mut below = vec![dir.clone()];
        while let Some(at) = below.pop() {
            for (path, is_dir) in self.fs.read_dir(&at)? {
                let name = path
                    .strip_prefix(&self.root)
                    .expect("walked entries live under the root")
                    .to_string_lossy()
                    .into_owned();
                if is_dir && name.starts_with(prefix) {
                    sync_dir_named(&*self.fs, &path, DISK_OWED)?;
                    below.push(path);
                }
            }
        }
        sync_up(&*self.fs, &dir, &chain_end(&self.root), DISK_OWED)
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

/// What an unsynced directory costs an object, for the error that names it.
const OBJECT_OWED: &str = "an object is durable only once every directory from the bucket \
                           root's parent down to it has been fsynced (ObjectStore's contract)";

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
/// **How the contract is honoured, and what it costs here.** The trait owes
/// two things: a PUT is atomic, and an object a GET or LIST returns, like an
/// acknowledged PUT, is durable. A real object store gives the second for
/// free; a filesystem does not, because an object here is a directory entry
/// and directory entries are page cache until fsynced. `rename(2)` is atomic
/// against a process dying, but neither the renamed file's bytes nor any
/// entry on the path to it is durable against a power loss until fsynced.
///
/// So a PUT writes `<key>.tmp`, fsyncs it, renames it onto `<key>`, then
/// fsyncs the key's directory and every directory above it up to the bucket
/// root's parent (`sync_up`). Dropping the first fsync lets a power loss
/// leave the key holding zeros; dropping any directory fsync lets it lose an
/// acknowledged object, and a whole new generation prefix with it. The whole
/// chain on EVERY PUT, not only the directories this PUT created: a PUT that
/// created them and then failed leaves them existing but not durable, and a
/// retry that trusted their existence would be acknowledged over entries the
/// next power loss drops. Existing is not being durable. A PUT creates the
/// bucket root when it is absent and nothing above it, so every directory it
/// creates is on that chain: a bucket root whose parent does not exist is
/// refused by name on every PUT, as `ProdDisk` refuses a data directory.
///
/// A GET and a LIST sync before they return (DESIGN §2's "readable is not
/// durable", one layer up): a GET fsyncs the object and the same directory
/// chain, a LIST every directory it walks, every object it lists and the
/// root's parent. Without that, a PUT that failed after its rename — not
/// acknowledged, and so never retried by a process that died — is readable
/// from the page cache by the next process, which can act on an object the
/// next power loss takes away (an archive adopting a manifest the bucket
/// then loses).
///
/// The cost is one fsync per directory level per operation, on a store whose
/// PUTs carry whole WAL segments and snapshot batches and whose GETs are a
/// manifest per swap and one per object at restore. Held by the crash model
/// in this module's tests (`CrashFs`): a power cut after every filesystem
/// call of a workload, and one pin per sync above. A DELETE is not made
/// durable — the trait owes nothing for one, and a power loss may bring a
/// deleted object back.
pub struct FsObjectStore {
    root: PathBuf,
    fs: Box<dyn Fs>,
    /// One operation in flight at a time (the trait's own words). `RefCell`,
    /// not `Mutex`: `ObjectStore` requires only `Send`, and `RefCell<T>` is
    /// `Send` whenever `T` is.
    in_flight: RefCell<Option<ObjectOp>>,
}

impl FsObjectStore {
    /// The bucket root. Not created here — a store pointed at an absent
    /// directory answers `NotFound` for a `Get` and an empty `List`, which
    /// is what an empty bucket looks like. A PUT creates the root, and the
    /// directories under it a key needs, but never the root's parent: that
    /// must exist, or every PUT is refused by name.
    pub fn new(root: impl Into<PathBuf>) -> FsObjectStore {
        FsObjectStore {
            root: root.into(),
            fs: Box::new(StdFs),
            in_flight: RefCell::new(None),
        }
    }

    #[cfg(test)]
    fn with_fs(root: impl Into<PathBuf>, fs: Box<dyn Fs>) -> FsObjectStore {
        FsObjectStore {
            root: root.into(),
            fs,
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
        create_root(&*self.fs, &self.root, "bucket root", "an acknowledged PUT")?;
        create_under(&*self.fs, &self.root, parent)?;
        let tmp = path.with_file_name(format!(
            "{}{TMP_SUFFIX}",
            path.file_name()
                .expect("resolve rejects empty components")
                .to_string_lossy()
        ));
        let file = self.fs.create(&tmp)?;
        file.write_at(0, data)?;
        // Before the rename: the name must never point at bytes that are not
        // on the platter yet.
        file.sync()?;
        drop(file);
        self.fs.rename(&tmp, &path)?;
        // After the rename: the directory entry that makes the object exist
        // is itself a write, and so is every entry on the path down to it.
        sync_up(&*self.fs, parent, &chain_end(&self.root), OBJECT_OWED)
    }

    fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        let path = self.resolve(key);
        let file = match self.fs.open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("no object at key \"{key}\""),
                ));
            }
            Err(e) => return Err(e),
        };
        // Readable is not durable (DESIGN §2): a PUT that failed after its
        // rename leaves an object only the page cache holds, and the process
        // reading it may not be the one that wrote it. Sync, then read.
        file.sync()?;
        let mut bytes = vec![0; usize::try_from(file.len()?).expect("an object fits in memory")];
        file.read_at(0, &mut bytes)?;
        sync_up(
            &*self.fs,
            path.parent().expect("resolve joins onto the root"),
            &chain_end(&self.root),
            OBJECT_OWED,
        )?;
        Ok(bytes)
    }

    /// Every key under `prefix`, sorted — never filesystem enumeration
    /// order, the same rule `Disk::list` follows. A missing root is an empty
    /// bucket, not an error. Temp files a crashed PUT left behind are not
    /// objects and are never listed.
    ///
    /// **Walks the whole bucket; `prefix` is a post-filter on the assembled
    /// key, not a descent.** So one LIST is O(objects in the bucket), in
    /// reads and in fsyncs: every directory it walks and every object it
    /// lists is synced before the list is returned, and the root's parent
    /// too, so each listed key is durable down its whole path. Correct, and
    /// deliberate for v1 — a prefix here is an arbitrary key substring, not
    /// a path component, so descending into `root.join(prefix)` would answer
    /// a different question — and no product path LISTs today (the archive
    /// and restore name every object they read). It is the first thing to
    /// change if a bucket ever holds many generations.
    fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut stack = vec![self.root.clone()];
        let mut walked = false;
        while let Some(dir) = stack.pop() {
            let entries = match self.fs.read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            walked = true;
            sync_dir_named(&*self.fs, &dir, OBJECT_OWED)?;
            for (path, is_dir) in entries {
                if is_dir {
                    stack.push(path);
                    continue;
                }
                let key = path
                    .strip_prefix(&self.root)
                    .expect("walked entries live under the root")
                    .to_string_lossy()
                    .into_owned();
                if !key.ends_with(TMP_SUFFIX) && key.starts_with(prefix) {
                    self.fs.open(&path)?.sync()?;
                    keys.push(key);
                }
            }
        }
        if walked && let Some(parent) = parent_dir(&self.root) {
            sync_dir_named(&*self.fs, &parent, OBJECT_OWED)?;
        }
        keys.sort();
        Ok(keys)
    }

    fn delete(&self, key: &str) -> io::Result<()> {
        match self.fs.remove_file(&self.resolve(key)) {
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

    // ------------------------------------------- FsObjectStore's crash model

    use std::collections::BTreeMap;
    use std::path::Component;
    use std::sync::{Arc, Mutex};

    /// What a power loss keeps, for the calls `ProdDisk` and `FsObjectStore`
    /// make and no others: the POSIX minimum, with nothing a journal's
    /// ordering might add. A file's bytes survive once a sync of that file
    /// returns `Ok`. A directory's entries survive once a sync of that
    /// directory returns `Ok` — each directory on its own, so a new
    /// subdirectory's entry needs its parent synced. A power loss drops
    /// everything else, all of it, every time (the seam's doc, `Fs`, says
    /// what that cannot find). A process restart drops nothing, because the
    /// page cache outlives the process: a fresh `ProdDisk` or `FsObjectStore`
    /// over the same `CrashFs` is that restart. A failed sync makes nothing
    /// durable, the worst case fsyncgate allows; a later sync of the same
    /// node succeeds honestly, as the simulator's disk does (DESIGN §1).
    #[derive(Clone)]
    struct CrashFs(Arc<Mutex<Model>>);

    struct Model {
        /// Inode 0 is `/`, which is durable from the start.
        nodes: Vec<Node>,
        /// One-shot: the next sync of each of these paths fails.
        failing_syncs: Vec<PathBuf>,
        /// Calls left before the power goes out; once it reaches 0, every
        /// call fails until `power_loss`.
        calls_left: Option<u64>,
        calls: u64,
    }

    enum Node {
        File {
            live: Vec<u8>,
            durable: Vec<u8>,
        },
        Dir {
            live: BTreeMap<String, usize>,
            durable: BTreeMap<String, usize>,
        },
    }

    impl Model {
        fn tick(&mut self) -> io::Result<()> {
            self.calls += 1;
            match &mut self.calls_left {
                None => Ok(()),
                Some(0) => Err(io::Error::other("CrashFs: the power is out")),
                Some(n) => {
                    *n -= 1;
                    Ok(())
                }
            }
        }

        fn names(path: &Path) -> Vec<String> {
            path.components()
                .filter_map(|c| match c {
                    Component::RootDir => None,
                    Component::Normal(n) => Some(n.to_string_lossy().into_owned()),
                    other => panic!(
                        "CrashFs takes absolute, normalised paths — got {other:?} in {}",
                        path.display()
                    ),
                })
                .collect()
        }

        fn lookup(&self, path: &Path) -> Option<usize> {
            let mut at = 0;
            for name in Self::names(path) {
                match &self.nodes[at] {
                    Node::Dir { live, .. } => at = *live.get(&name)?,
                    Node::File { .. } => return None,
                }
            }
            Some(at)
        }

        fn not_found(path: &Path) -> io::Error {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("CrashFs: nothing at {}", path.display()),
            )
        }

        /// The directory holding `path`, and `path`'s name in it.
        fn parent_of(&self, path: &Path) -> io::Result<(usize, String)> {
            let parent = path.parent().expect("CrashFs: `/` has no parent");
            let name = path
                .file_name()
                .expect("CrashFs: a path below `/` has a name")
                .to_string_lossy()
                .into_owned();
            match self.lookup(parent) {
                Some(dir) if matches!(self.nodes[dir], Node::Dir { .. }) => Ok((dir, name)),
                _ => Err(Self::not_found(parent)),
            }
        }

        fn entries(&mut self, dir: usize) -> &mut BTreeMap<String, usize> {
            match &mut self.nodes[dir] {
                Node::Dir { live, .. } => live,
                Node::File { .. } => panic!("CrashFs: inode {dir} is not a directory"),
            }
        }

        /// A sync of the node at `ino`, reached as `path`.
        fn make_durable(&mut self, ino: usize, path: &Path) -> io::Result<()> {
            if let Some(i) = self.failing_syncs.iter().position(|p| p == path) {
                self.failing_syncs.remove(i);
                return Err(io::Error::other(format!(
                    "CrashFs: injected fsync failure on {}",
                    path.display()
                )));
            }
            match &mut self.nodes[ino] {
                Node::File { live, durable } => *durable = live.clone(),
                Node::Dir { live, durable } => *durable = live.clone(),
            }
            Ok(())
        }
    }

    impl CrashFs {
        fn new() -> CrashFs {
            CrashFs(Arc::new(Mutex::new(Model {
                nodes: vec![Node::Dir {
                    live: BTreeMap::new(),
                    durable: BTreeMap::new(),
                }],
                failing_syncs: Vec::new(),
                calls_left: None,
                calls: 0,
            })))
        }

        fn model(&self) -> std::sync::MutexGuard<'_, Model> {
            self.0
                .lock()
                .expect("CrashFs lock poisoned by an earlier panic")
        }

        /// Everything a sync never took is gone, and the power is back on.
        fn power_loss(&self) {
            let mut m = self.model();
            for node in &mut m.nodes {
                match node {
                    Node::File { live, durable } => *live = durable.clone(),
                    Node::Dir { live, durable } => *live = durable.clone(),
                }
            }
            m.calls_left = None;
        }

        fn fail_next_sync(&self, path: &str) {
            self.model().failing_syncs.push(PathBuf::from(path));
        }

        fn power_out_after(&self, calls: u64) {
            self.model().calls_left = Some(calls);
        }

        fn calls(&self) -> u64 {
            self.model().calls
        }

        /// `open(2)` with `O_CREAT`, and `O_TRUNC` when `truncate`.
        fn open_creating(&self, path: &Path, truncate: bool) -> io::Result<Box<dyn File>> {
            let mut m = self.model();
            m.tick()?;
            let (dir, name) = m.parent_of(path)?;
            let ino = match m.entries(dir).get(&name).copied() {
                Some(ino) => {
                    match &mut m.nodes[ino] {
                        Node::File { live, .. } if truncate => live.clear(),
                        Node::File { .. } => {}
                        Node::Dir { .. } => {
                            return Err(io::Error::other("CrashFs: is a directory"));
                        }
                    }
                    ino
                }
                None => {
                    let ino = m.nodes.len();
                    m.nodes.push(Node::File {
                        live: Vec::new(),
                        durable: Vec::new(),
                    });
                    m.entries(dir).insert(name, ino);
                    ino
                }
            };
            Ok(Box::new(CrashFile {
                fs: self.clone(),
                ino,
                path: path.to_path_buf(),
            }))
        }

        /// Directories that exist durably before the store sees them, as an
        /// operator's would: `dir` and every directory above it.
        fn provisioned(dir: &str) -> CrashFs {
            let fs = CrashFs::new();
            let dir = Path::new(dir);
            for level in dir
                .ancestors()
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .skip(1)
            {
                fs.create_dir(level).unwrap();
            }
            for level in dir.ancestors() {
                fs.sync_dir(level).unwrap();
            }
            fs
        }
    }

    impl Fs for CrashFs {
        fn create_dir(&self, dir: &Path) -> io::Result<()> {
            let mut m = self.model();
            m.tick()?;
            let exists = || {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("CrashFs: {} exists", dir.display()),
                )
            };
            if dir.parent().is_none() {
                return Err(exists()); // `/`
            }
            let (parent, name) = m.parent_of(dir)?;
            if m.entries(parent).contains_key(&name) {
                return Err(exists());
            }
            let ino = m.nodes.len();
            m.nodes.push(Node::Dir {
                live: BTreeMap::new(),
                durable: BTreeMap::new(),
            });
            m.entries(parent).insert(name, ino);
            Ok(())
        }

        /// A stat with the power out answers nothing, so `create_root` and
        /// `create_under` cannot mistake the outage for a directory that
        /// already exists.
        fn is_dir(&self, path: &Path) -> bool {
            let mut m = self.model();
            m.tick().is_ok()
                && m.lookup(path)
                    .is_some_and(|ino| matches!(m.nodes[ino], Node::Dir { .. }))
        }

        fn open_rw(&self, path: &Path) -> io::Result<Box<dyn File>> {
            self.open_creating(path, false)
        }

        fn create(&self, path: &Path) -> io::Result<Box<dyn File>> {
            self.open_creating(path, true)
        }

        fn open(&self, path: &Path) -> io::Result<Box<dyn File>> {
            let mut m = self.model();
            m.tick()?;
            match m.lookup(path) {
                Some(ino) if matches!(m.nodes[ino], Node::File { .. }) => Ok(Box::new(CrashFile {
                    fs: self.clone(),
                    ino,
                    path: path.to_path_buf(),
                })),
                _ => Err(Model::not_found(path)),
            }
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let mut m = self.model();
            m.tick()?;
            let (from_dir, from_name) = m.parent_of(from)?;
            let (to_dir, to_name) = m.parent_of(to)?;
            let ino = m
                .entries(from_dir)
                .remove(&from_name)
                .ok_or_else(|| Model::not_found(from))?;
            m.entries(to_dir).insert(to_name, ino);
            Ok(())
        }

        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            let mut m = self.model();
            m.tick()?;
            match m.lookup(dir) {
                Some(ino) if matches!(m.nodes[ino], Node::Dir { .. }) => m.make_durable(ino, dir),
                _ => Err(Model::not_found(dir)),
            }
        }

        fn read_dir(&self, dir: &Path) -> io::Result<Vec<(PathBuf, bool)>> {
            let mut m = self.model();
            m.tick()?;
            match m.lookup(dir).map(|ino| &m.nodes[ino]) {
                Some(Node::Dir { live, .. }) => Ok(live
                    .iter()
                    .map(|(name, &ino)| (dir.join(name), matches!(m.nodes[ino], Node::Dir { .. })))
                    .collect()),
                _ => Err(Model::not_found(dir)),
            }
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            let mut m = self.model();
            m.tick()?;
            let (dir, name) = m.parent_of(path)?;
            match m.entries(dir).remove(&name) {
                Some(_) => Ok(()),
                None => Err(Model::not_found(path)),
            }
        }
    }

    struct CrashFile {
        fs: CrashFs,
        ino: usize,
        path: PathBuf,
    }

    impl CrashFile {
        /// The file's live bytes, after the call is counted.
        fn live(&self, op: impl FnOnce(&mut Vec<u8>) -> io::Result<()>) -> io::Result<()> {
            let mut m = self.fs.model();
            m.tick()?;
            match &mut m.nodes[self.ino] {
                Node::File { live, .. } => op(live),
                Node::Dir { .. } => unreachable!("CrashFile always names a file"),
            }
        }
    }

    impl File for CrashFile {
        fn len(&self) -> io::Result<u64> {
            let mut len = 0;
            self.live(|live| {
                len = live.len() as u64;
                Ok(())
            })?;
            Ok(len)
        }

        fn read_at(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            self.live(|live| {
                let at = offset as usize;
                match live.get(at..at + out.len()) {
                    Some(bytes) => {
                        out.copy_from_slice(bytes);
                        Ok(())
                    }
                    None => Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "CrashFs: a read past the end of the file",
                    )),
                }
            })
        }

        fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.live(|live| {
                let at = offset as usize;
                if live.len() < at + data.len() {
                    live.resize(at + data.len(), 0);
                }
                live[at..at + data.len()].copy_from_slice(data);
                Ok(())
            })
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.live(|live| {
                live.resize(len as usize, 0);
                Ok(())
            })
        }

        fn sync(&self) -> io::Result<()> {
            let mut m = self.fs.model();
            m.tick()?;
            m.make_durable(self.ino, &self.path)
        }
    }

    const ROOT: &str = "/bucket";

    /// A process restart: a fresh store over the same page cache.
    fn store(fs: &CrashFs) -> FsObjectStore {
        FsObjectStore::with_fs(ROOT, Box::new(fs.clone()))
    }

    fn run(s: &FsObjectStore, op: ObjectOp) -> io::Result<ObjectDone> {
        s.start(op).unwrap();
        s.poll()
            .expect("FsObjectStore completes every op on its first poll")
    }

    fn put(s: &FsObjectStore, key: &str, data: &[u8]) -> io::Result<()> {
        run(
            s,
            ObjectOp::Put {
                key: key.to_string(),
                data: data.to_vec(),
            },
        )
        .map(|done| assert!(matches!(done, ObjectDone::Put), "{done:?}"))
    }

    /// Every object these tests write is text, so a failure prints words.
    fn get(s: &FsObjectStore, key: &str) -> io::Result<String> {
        match run(
            s,
            ObjectOp::Get {
                key: key.to_string(),
            },
        )? {
            ObjectDone::Get(bytes) => Ok(String::from_utf8(bytes).expect("test objects are text")),
            other => panic!("a Get completed as {other:?}"),
        }
    }

    fn list(s: &FsObjectStore, prefix: &str) -> io::Result<Vec<String>> {
        match run(
            s,
            ObjectOp::List {
                prefix: prefix.to_string(),
            },
        )? {
            ObjectDone::List(keys) => Ok(keys),
            other => panic!("a List completed as {other:?}"),
        }
    }

    /// A file whose name is durable and whose bytes are not: what a copy into
    /// the bucket that never fsynced leaves once something synced the
    /// directories above it. `FsObjectStore` never makes one itself (it syncs
    /// a temp file before renaming it into place), but its reads owe the
    /// contract for every object they return, not only for the ones it wrote.
    fn plant_unsynced_bytes(path: &str, data: &[u8]) -> CrashFs {
        let path = Path::new(path);
        let fs = CrashFs::provisioned(path.parent().unwrap().to_str().unwrap());
        fs.create(path).unwrap().write_at(0, data).unwrap();
        fs.sync_dir(path.parent().unwrap()).unwrap();
        fs
    }

    /// (a), a GET. A PUT whose directory fsync failed was not acknowledged,
    /// but its rename had happened, so the page cache holds the new object.
    /// A restarted archiver's opening GET of the manifest reads it back and
    /// acts on it; the next power loss reverts the manifest to the old one.
    /// An object a GET returns must survive the next power loss.
    #[test]
    fn an_object_a_get_returned_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned("/bucket/p");
        let s = store(&fs);
        put(&s, "p/manifest", b"generation 1").unwrap();
        fs.fail_next_sync("/bucket/p");
        let err = put(&s, "p/manifest", b"generation 2").unwrap_err();
        assert!(err.to_string().contains("injected fsync failure"), "{err}");

        let s = store(&fs); // the process restarts
        let seen = get(&s, "p/manifest").unwrap();
        assert_eq!(
            seen, "generation 2",
            "the restarted store reads the unacknowledged PUT back from the page cache — if \
             not, this test no longer reaches the shape it pins"
        );
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "p/manifest").unwrap(),
            seen,
            "an object a GET returned changed at the next power loss — FsObjectStore served a \
             name it never made durable"
        );
    }

    /// (a), a GET whose object sits under a directory a failed PUT created:
    /// the key's own directory was synced, the entry for the new prefix in
    /// the root was not. A GET syncs every directory up to the root.
    #[test]
    fn an_object_a_get_returned_under_a_prefix_a_failed_put_created_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned(ROOT);
        fs.fail_next_sync("/bucket");
        put(&store(&fs), "p/0/segments/a", b"sealed").expect_err(
            "the PUT's sync of the bucket root must fail — if not, this test no longer reaches \
             the shape it pins",
        );
        let seen = get(&store(&fs), "p/0/segments/a").unwrap();
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "p/0/segments/a").unwrap_or_else(|e| panic!(
                "an object a GET returned vanished at the next power loss — a directory above \
                 its key was never made durable: {e}"
            )),
            seen
        );
    }

    /// (a), a GET under a root a failed PUT created: only the root's own
    /// entry, in the root's parent, was never synced.
    #[test]
    fn an_object_a_get_returned_under_a_root_a_failed_put_created_survives_the_next_power_loss() {
        let fs = CrashFs::new();
        fs.fail_next_sync("/");
        put(&store(&fs), "k", b"v").expect_err(
            "the PUT's sync of the root's parent must fail — if not, this test no longer reaches \
             the shape it pins",
        );
        let seen = get(&store(&fs), "k").unwrap();
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "k").unwrap_or_else(|e| panic!(
                "an object a GET returned vanished at the next power loss — the bucket root's own \
                 entry was never made durable: {e}"
            )),
            seen
        );
    }

    /// (a), a LIST: the same shape for a key that did not exist before.
    #[test]
    fn a_key_a_list_returned_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned("/bucket/p/0/segments");
        let s = store(&fs);
        put(&s, "p/0/segments/a", b"a").unwrap();
        fs.fail_next_sync("/bucket/p/0/segments");
        put(&s, "p/0/segments/b", b"b").unwrap_err();

        let s = store(&fs);
        let seen = list(&s, "p/").unwrap();
        assert_eq!(
            seen,
            ["p/0/segments/a", "p/0/segments/b"],
            "the restarted store lists the unacknowledged PUT from the page cache — if not, this \
             test no longer reaches the shape it pins"
        );
        fs.power_loss();
        assert_eq!(
            list(&store(&fs), "p/").unwrap(),
            seen,
            "a key a LIST returned vanished at the next power loss — FsObjectStore listed a name \
             it never made durable"
        );
    }

    /// (a), a LIST under a root that a failed PUT created: the root's own
    /// entry lives in the root's parent, which only that PUT would have
    /// synced.
    #[test]
    fn a_key_a_list_returned_under_a_root_a_failed_put_created_survives_the_next_power_loss() {
        let fs = CrashFs::new();
        fs.fail_next_sync("/bucket");
        put(&store(&fs), "k", b"v").unwrap_err();

        let s = store(&fs);
        let seen = list(&s, "").unwrap();
        assert_eq!(
            seen,
            ["k"],
            "the restarted store lists the unacknowledged PUT from the page cache — if not, this \
             test no longer reaches the shape it pins"
        );
        fs.power_loss();
        assert_eq!(
            list(&store(&fs), "").unwrap(),
            seen,
            "a key a LIST returned vanished at the next power loss — the bucket root's own \
             entry was never made durable"
        );
    }

    /// (a), the bytes behind a name. A GET returns them, so they must be the
    /// bytes the next power loss leaves.
    #[test]
    fn the_bytes_a_get_returned_survive_the_next_power_loss() {
        let fs = plant_unsynced_bytes("/bucket/k", b"copied in");
        let seen = get(&store(&fs), "k").unwrap();
        assert_eq!(seen, "copied in");
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "k").unwrap(),
            seen,
            "the bytes a GET returned changed at the next power loss — FsObjectStore returned \
             bytes it never synced"
        );
    }

    /// The same for a LIST: it returns the name, so the object behind it —
    /// bytes included — is durable before the name is returned.
    #[test]
    fn the_object_behind_a_key_a_list_returned_survives_the_next_power_loss() {
        let fs = plant_unsynced_bytes("/bucket/k", b"copied in");
        assert_eq!(list(&store(&fs), "").unwrap(), ["k"]);
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "k").unwrap(),
            "copied in",
            "the object behind a key a LIST returned changed at the next power loss — \
             FsObjectStore listed an object whose bytes it never synced"
        );
    }

    /// (b). An acknowledged PUT under a prefix that did not exist — a new
    /// generation's first object — survives a power loss: every directory the
    /// PUT created has its entry synced into its parent.
    #[test]
    fn an_acked_put_under_a_new_prefix_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned(ROOT);
        put(&store(&fs), "p/0/segments/a", b"sealed").unwrap();
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "p/0/segments/a").unwrap_or_else(|e| panic!(
                "an acknowledged PUT under a new prefix was lost at the next power loss — a \
                 directory it created was never synced into its parent: {e}"
            )),
            "sealed"
        );
    }

    /// (b), and why it is not enough to sync only the directories a PUT
    /// creates: a PUT that created them and failed before syncing them, then
    /// a retry that finds them already there. Existing is not being durable.
    #[test]
    fn an_acked_retry_under_a_prefix_a_failed_put_created_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned(ROOT);
        fs.fail_next_sync("/bucket");
        put(&store(&fs), "p/0/segments/a", b"sealed").expect_err(
            "the first PUT's sync of the bucket root must fail — if not, this test no longer \
             reaches the shape it pins",
        );
        put(&store(&fs), "p/0/segments/a", b"sealed").unwrap(); // a restarted process retries
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "p/0/segments/a").unwrap_or_else(|e| panic!(
                "an acknowledged retry was lost at the next power loss — it trusted directories \
                 an earlier, failed PUT created and never synced: {e}"
            )),
            "sealed"
        );
    }

    /// (b), the root: a PUT creates the bucket root when it is absent, and
    /// the root's own entry lives in the root's parent.
    #[test]
    fn an_acked_put_that_created_the_bucket_root_survives_the_next_power_loss() {
        let fs = CrashFs::new();
        put(&store(&fs), "k", b"v").unwrap();
        fs.power_loss();
        assert_eq!(
            get(&store(&fs), "k").unwrap_or_else(|e| panic!(
                "an acknowledged PUT that created the bucket root was lost at the next power loss \
                 — the root's own entry was never synced into its parent: {e}"
            )),
            "v"
        );
    }

    /// The bucket root's parent must exist: a PUT creates the bucket root
    /// and nothing above it, and refuses by name rather than create a
    /// directory whose entry no PUT's chain reaches.
    #[test]
    fn a_put_under_a_bucket_root_whose_parent_is_missing_is_refused_by_name_and_nothing_is_created()
    {
        let fs = CrashFs::new();
        let s = FsObjectStore::with_fs("/backups/bucket", Box::new(fs.clone()));
        let err = put(&s, "k", b"v").expect_err("a PUT created /backups to hold the bucket root");
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        assert_eq!(
            err.to_string(),
            "object PUT \"k\" failed: the bucket root \"/backups/bucket\" cannot be created: its parent \"/backups\" does \
             not exist, and charpente never creates a directory above the bucket root — an \
             acknowledged PUT rests on every directory entry above it, and charpente vouches only \
             for the entries it creates and fsyncs. Create \"/backups\" (mkdir -p \"/backups\"), \
             or point at a bucket root whose parent exists"
        );
        assert!(
            !fs.is_dir(Path::new("/backups")),
            "the refusal created /backups"
        );
    }

    /// The class property, by sweep: the power goes out after every possible
    /// number of filesystem calls in a fixed workload — a PUT that creates the
    /// root, PUTs under new prefixes, an overwrite — and after it, every
    /// acknowledged PUT holds its bytes, and a key whose PUT was cut holds its
    /// previous value or its new one, never anything else (the atomic PUT).
    /// Nothing is listed that a GET cannot read.
    #[test]
    fn every_power_cut_keeps_acked_puts_and_leaves_cut_ones_atomic() {
        let workload = [
            ("p/manifest", "m1"),
            ("p/0/segments/a", "seg-a"),
            ("p/manifest", "m2"),
            ("p/1/snapshot/meta", "meta"),
        ];
        let total = {
            let fs = CrashFs::new();
            let s = store(&fs);
            for (key, data) in workload {
                put(&s, key, data.as_bytes()).unwrap();
            }
            fs.calls()
        };
        assert!(
            total > 20,
            "the workload makes only {total} filesystem calls — the sweep has nothing to cut"
        );
        for cut in 0..=total {
            let fs = CrashFs::new();
            fs.power_out_after(cut);
            let s = store(&fs);
            // key -> (last acknowledged value, value of a PUT the cut interrupted)
            let mut expect: BTreeMap<&str, (Option<&str>, Option<&str>)> = BTreeMap::new();
            for (key, data) in workload {
                let entry = expect.entry(key).or_default();
                match put(&s, key, data.as_bytes()) {
                    Ok(()) => *entry = (Some(data), None),
                    Err(_) => {
                        entry.1 = Some(data);
                        break;
                    }
                }
            }
            fs.power_loss();
            let s = store(&fs);
            for (key, (acked, cut_off)) in &expect {
                let found = match get(&s, key) {
                    Ok(bytes) => Some(bytes),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                    Err(e) => panic!("cut {cut}: GET \"{key}\" failed: {e}"),
                };
                let allowed: Vec<Option<&str>> = [Some(*acked), cut_off.map(Some)]
                    .into_iter()
                    .flatten()
                    .collect();
                assert!(
                    allowed.contains(&found.as_deref()),
                    "cut after {cut} of {total} filesystem calls: \"{key}\" holds {found:?} — \
                     allowed {allowed:?} (an acknowledged PUT must survive, and a cut one is \
                     atomic)"
                );
            }
            for key in list(&s, "").unwrap() {
                get(&s, &key).unwrap_or_else(|e| {
                    panic!("cut {cut}: LIST named \"{key}\", which GET cannot read: {e}")
                });
            }
        }
    }

    // ---------------------------------------------------- ProdDisk's crash model

    use crate::kernel::cron::CronRow;
    use charpente_core::db::{Db, DbConfig, Schema};
    use charpente_core::table::{Id, View};

    const NODE: &str = "/srv/node";

    /// One boot of a node over the page cache `fs` holds: its data directory,
    /// then `Db::open` — every filesystem call a fresh node makes before it
    /// can acknowledge a write.
    fn boot(fs: &CrashFs) -> io::Result<Db> {
        let disk = ProdDisk::over(PathBuf::from(NODE), Box::new(fs.clone()))?;
        // Any `#[table]` will do; cron's is the platform's own global one.
        Db::open(
            Box::new(disk),
            DbConfig::default(),
            Schema::new().table::<CronRow>(),
        )
    }

    /// `transact` returns `Ok` once the write's WAL frame is fsynced: an ack.
    fn write(db: &mut Db, id: u64, name: &str) -> Result<(), String> {
        db.transact(|tx| {
            tx.put(&CronRow {
                id: Id::from_raw(id),
                name: name.to_string(),
                interval_nanos: 1,
                next_due: 1,
            });
            Ok::<(), io::Error>(())
        })
        .map_err(|e| format!("{e:?}"))
    }

    /// The power goes out under a running node: nothing it does from here
    /// reaches the disk, including what a clean shutdown would have written
    /// (redb commits the store durably when its `Database` drops).
    fn power_cut(fs: &CrashFs, db: Db) {
        fs.power_out_after(0);
        drop(db);
    }

    /// The rows a node finds when it boots after the next power loss.
    fn rows_after_power_loss(fs: &CrashFs) -> Vec<String> {
        fs.power_loss();
        let db = boot(fs).expect("a node boots after a power loss");
        db.snapshot()
            .unwrap()
            .scan::<CronRow>()
            .unwrap()
            .into_iter()
            .map(|row| row.name)
            .collect()
    }

    /// The fresh node. `Db::open` syncs the data directory when it creates
    /// the store, before the log's `wal/` exists; the log's own barrier then
    /// covers the names inside `wal/` and not `wal/`'s entry in the data
    /// directory. Every write acknowledged before the first checkpoint
    /// rewrote that directory rests on an entry the next power loss drops.
    #[test]
    fn an_acked_write_on_a_fresh_node_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned(NODE); // the operator's, already durable
        let mut db = boot(&fs).unwrap();
        write(&mut db, 0, "first").unwrap();
        power_cut(&fs, db);
        assert_eq!(
            rows_after_power_loss(&fs),
            ["first"],
            "a write acknowledged on a fresh node was lost at the next power loss"
        );
    }

    /// The data directory's own entry, in its parent, which nothing synced.
    #[test]
    fn an_acked_write_on_a_node_that_created_its_data_directory_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned("/srv");
        let mut db = boot(&fs).unwrap();
        write(&mut db, 0, "first").unwrap();
        power_cut(&fs, db);
        assert_eq!(rows_after_power_loss(&fs), ["first"]);
    }

    /// The data directory's parent must exist: `ProdDisk::new` creates the
    /// data directory and nothing above it, and refuses by name rather than
    /// create a directory whose entry no barrier's chain reaches.
    #[test]
    fn a_data_directory_whose_parent_is_missing_is_refused_by_name_and_nothing_is_created() {
        let fs = CrashFs::provisioned("/");
        let Err(err) = ProdDisk::over(PathBuf::from(NODE), Box::new(fs.clone())) else {
            panic!("ProdDisk::new created /srv to hold the data directory /srv/node")
        };
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        assert_eq!(
            err.to_string(),
            "the data directory \"/srv/node\" cannot be created: its parent \"/srv\" does not \
             exist, and charpente never creates a directory above the data directory — an \
             acknowledged write rests on every directory entry above it, and charpente vouches \
             only for the entries it creates and fsyncs. Create \"/srv\" (mkdir -p \"/srv\"), or \
             point at a data directory whose parent exists"
        );
        assert!(!fs.is_dir(Path::new("/srv")), "the refusal created /srv");
    }

    /// Why every barrier syncs the whole chain, not only what its own call
    /// created: the boot that created the data directory failed to make its
    /// entry durable, and the next boot finds the directory there and
    /// creates nothing. Only a barrier that re-syncs what already exists
    /// keeps that boot's acknowledged writes. Two failures, because the
    /// first boot meets the parent twice: at the store's creation barrier,
    /// and again at the barrier of the discard-and-rebuild `Db::open` runs
    /// when a store will not open.
    #[test]
    fn an_acked_write_after_a_boot_whose_directory_sync_failed_survives_the_next_power_loss() {
        let fs = CrashFs::provisioned("/srv");
        fs.fail_next_sync("/srv");
        fs.fail_next_sync("/srv");
        let Err(err) = boot(&fs) else {
            panic!(
                "the first boot syncs the directory holding the data directory it created, and \
                 that sync fails"
            )
        };
        assert!(err.to_string().contains("injected fsync failure"), "{err}");
        assert!(
            err.to_string()
                .contains("could not fsync directory \"/srv\""),
            "{err}"
        );
        assert!(
            err.to_string().contains("Disk::sync_dir's contract"),
            "{err}"
        );

        let mut db = boot(&fs).expect("the next boot finds the data directory in the page cache");
        write(&mut db, 0, "first").unwrap();
        power_cut(&fs, db);
        assert_eq!(rows_after_power_loss(&fs), ["first"]);
    }

    /// `Disk::sync_dir`'s contract is every name starting with the prefix,
    /// and a flat name can sit in a directory below the one holding the
    /// prefix's last component. No platform name does today; `SimDisk` keeps
    /// one durable all the same, so `ProdDisk` must.
    #[test]
    fn a_barrier_keeps_every_name_under_its_prefix_however_deep() {
        let fs = CrashFs::provisioned(NODE);
        let disk = ProdDisk::over(PathBuf::from(NODE), Box::new(fs.clone())).unwrap();
        let file = disk.open("archive/0/segment").unwrap();
        file.write_at(0, b"bytes").unwrap();
        file.sync().unwrap();
        disk.sync_dir("archive/").unwrap();
        drop((file, disk));
        fs.power_loss();
        let disk = ProdDisk::over(PathBuf::from(NODE), Box::new(fs.clone())).unwrap();
        assert_eq!(disk.list("").unwrap(), ["archive/0/segment"]);
    }

    /// A barrier that cannot fsync a directory on its chain says which one,
    /// what waits on it and by whose contract, and makes nothing durable:
    /// the name it was asked to keep is lost at the next power loss.
    #[test]
    fn a_failed_barrier_names_the_directory_and_the_contract() {
        let fs = CrashFs::provisioned(NODE);
        let disk = ProdDisk::over(PathBuf::from(NODE), Box::new(fs.clone())).unwrap();
        let file = disk.open("wal/0").unwrap();
        file.sync().unwrap();
        fs.fail_next_sync(NODE);
        let err = disk.sync_dir("wal/").unwrap_err().to_string();
        assert!(
            err.starts_with(
                "could not fsync directory \"/srv/node\" (CrashFs: injected fsync \
                             failure on /srv/node) — a name is durable only once the directory \
                             holding it and every directory above it, up to the data \
                             directory's parent, have been fsynced (Disk::sync_dir's contract); \
                             check that directory's permissions and the disk under it"
            ),
            "{err}"
        );
        drop((file, disk));
        fs.power_loss();
        let disk = ProdDisk::over(PathBuf::from(NODE), Box::new(fs.clone())).unwrap();
        assert_eq!(disk.list("").unwrap(), Vec::<String>::new());
    }

    /// A power cut after every filesystem call a fresh node makes, from
    /// creating its data directory through three acknowledged writes, and
    /// after the last. After each: the node boots, holds every write it
    /// acknowledged, and holds a cut write at most whole.
    #[test]
    fn every_power_cut_on_a_fresh_node_keeps_its_acked_writes() {
        let workload = ["first", "second", "third"];
        let total = {
            let fs = CrashFs::provisioned("/srv");
            let mut db = boot(&fs).unwrap();
            for (id, name) in workload.iter().enumerate() {
                write(&mut db, id as u64, name).unwrap();
            }
            fs.calls()
        };
        for cut in 0..=total {
            let fs = CrashFs::provisioned("/srv");
            fs.power_out_after(cut);
            let mut acked = Vec::new();
            let mut cut_off = None;
            if let Ok(mut db) = boot(&fs) {
                for (id, name) in workload.iter().enumerate() {
                    match write(&mut db, id as u64, name) {
                        Ok(()) => acked.push(*name),
                        Err(_) => {
                            cut_off = Some(*name);
                            break;
                        }
                    }
                }
                power_cut(&fs, db);
            }
            let found = rows_after_power_loss(&fs);
            let allowed_extra: Vec<&str> = cut_off.into_iter().collect();
            assert!(
                found.len() >= acked.len()
                    && found[..acked.len()] == acked[..]
                    && found[acked.len()..]
                        .iter()
                        .all(|n| allowed_extra.contains(&n.as_str())),
                "cut after {cut} of {total} filesystem calls: the node holds {found:?} — it \
                 acknowledged {acked:?}, and the write the cut interrupted was {cut_off:?}"
            );
        }
    }
}
