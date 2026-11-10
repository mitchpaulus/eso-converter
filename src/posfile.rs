//! A file written at explicit offsets from many threads.
//!
//! Linux: `pwrite`. The page cache takes parallel positional writes well.
//!
//! Windows: the file is extended and memory-mapped in 64 MB segments and
//! writes are plain copies into the mapping. Measured on NTFS, `WriteFile`
//! is throttled by the cache manager once a few hundred MB are dirty (often
//! 10x slower), while mapped writes are not. If mapping fails (e.g. some
//! network file systems) it falls back to positional `WriteFile`.
use std::fs::File;
use std::io;
use std::path::Path;

pub struct PosFile {
    file: File,
    #[cfg(windows)]
    segs: std::sync::RwLock<Vec<memmap2::MmapMut>>,
    #[cfg(windows)]
    nomap: std::sync::atomic::AtomicBool,
}

#[cfg(windows)]
const SEG: u64 = 64 << 20;

impl PosFile {
    pub fn create(path: &Path) -> io::Result<PosFile> {
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
        Ok(PosFile {
            file,
            #[cfg(windows)]
            segs: std::sync::RwLock::new(Vec::new()),
            #[cfg(windows)]
            nomap: std::sync::atomic::AtomicBool::new(false),
        })
    }

    #[cfg(unix)]
    pub fn write_at(&self, off: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.file.write_all_at(data, off)
    }

    #[cfg(windows)]
    pub fn write_at(&self, off: u64, data: &[u8]) -> io::Result<()> {
        use std::os::windows::fs::FileExt;
        use std::sync::atomic::Ordering;
        if data.is_empty() {
            return Ok(());
        }
        let end = off + data.len() as u64;
        if !self.nomap.load(Ordering::Relaxed) {
            if (self.segs.read().unwrap().len() as u64) * SEG < end {
                let mut segs = self.segs.write().unwrap();
                while (segs.len() as u64) * SEG < end && !self.nomap.load(Ordering::Relaxed) {
                    let o = segs.len() as u64 * SEG;
                    let mapped = self
                        .file
                        .set_len(o + SEG)
                        .and_then(|_| unsafe { memmap2::MmapOptions::new().offset(o).len(SEG as usize).map_mut(&self.file) });
                    match mapped {
                        Ok(m) => segs.push(m),
                        Err(_) => self.nomap.store(true, Ordering::Relaxed),
                    }
                }
            }
            let segs = self.segs.read().unwrap();
            let mapped_end = segs.len() as u64 * SEG;
            if end <= mapped_end {
                let (mut o, mut d) = (off, data);
                while !d.is_empty() {
                    let s = (o / SEG) as usize;
                    let within = (o % SEG) as usize;
                    let n = d.len().min(SEG as usize - within);
                    // Regions written by different threads never overlap.
                    unsafe { std::ptr::copy_nonoverlapping(d.as_ptr(), (segs[s].as_ptr() as *mut u8).add(within), n) };
                    o += n as u64;
                    d = &d[n..];
                }
                return Ok(());
            }
        }
        let (mut o, mut d) = (off, data);
        while !d.is_empty() {
            let n = self.file.seek_write(d, o)?;
            o += n as u64;
            d = &d[n..];
        }
        Ok(())
    }

    /// Closes the file at its final length.
    pub fn finish(self, len: u64) -> io::Result<()> {
        #[cfg(windows)]
        drop(self.segs.into_inner().unwrap()); // a mapped file cannot be truncated
        let meta = self.file.metadata()?;
        if meta.is_file() && meta.len() != len {
            self.file.set_len(len)?;
        }
        Ok(())
    }
}
