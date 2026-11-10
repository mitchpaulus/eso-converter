//! Work-stealing over chunks with in-order consumption on the calling thread.
use crate::walk::WalkErr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

struct Shared<R> {
    slots: Vec<Option<R>>,
    next: usize,
    err: Option<WalkErr>,
}

/// Runs `work(i)` for `i in 0..n` on `threads` threads and feeds the results
/// to `consume` in index order. Workers never run more than `window` chunks
/// ahead of the consumer, which bounds memory.
pub fn run<R: Send>(
    n: usize,
    threads: usize,
    window: usize,
    work: &(dyn Fn(usize) -> Result<R, WalkErr> + Sync),
    consume: &mut dyn FnMut(usize, R) -> Result<(), WalkErr>,
) -> Result<(), WalkErr> {
    let sh = Mutex::new(Shared {
        slots: (0..n).map(|_| None).collect(),
        next: 0,
        err: None,
    });
    let done = Condvar::new();
    let room = Condvar::new();
    let counter = AtomicUsize::new(0);
    let fail = |e: WalkErr| {
        let mut g = sh.lock().unwrap();
        if g.err.is_none() {
            g.err = Some(e);
        }
        done.notify_all();
        room.notify_all();
    };
    std::thread::scope(|sc| {
        for _ in 0..threads.clamp(1, n.max(1)) {
            sc.spawn(|| loop {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                {
                    let mut g = sh.lock().unwrap();
                    while g.err.is_none() && i >= g.next.saturating_add(window) {
                        g = room.wait(g).unwrap();
                    }
                    if g.err.is_some() {
                        break;
                    }
                }
                match work(i) {
                    Ok(r) => {
                        sh.lock().unwrap().slots[i] = Some(r);
                        done.notify_all();
                    }
                    Err(e) => {
                        fail(e);
                        break;
                    }
                }
            });
        }
        for i in 0..n {
            let r = {
                let mut g = sh.lock().unwrap();
                loop {
                    if g.err.is_some() {
                        break None;
                    }
                    if let Some(r) = g.slots[i].take() {
                        break Some(r);
                    }
                    g = done.wait(g).unwrap();
                }
            };
            let Some(r) = r else { break };
            if let Err(e) = consume(i, r) {
                fail(e);
                break;
            }
            sh.lock().unwrap().next = i + 1;
            room.notify_all();
        }
    });
    match sh.into_inner().unwrap().err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Plain parallel-for without ordering.
pub fn par_for(n: usize, threads: usize, work: &(dyn Fn(usize) + Sync)) {
    let counter = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for _ in 0..threads.clamp(1, n.max(1)) {
            sc.spawn(|| loop {
                let i = counter.fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                work(i);
            });
        }
    });
}

pub const NOUT: usize = 7;

struct PosShared<R> {
    slots: Vec<Option<R>>,
    claimed: usize,
    /// Next chunk whose output offsets are still unknown.
    next: usize,
    offs: [u64; NOUT],
    ready: Vec<(usize, R, [u64; NOUT])>,
    writing: usize,
    err: Option<WalkErr>,
}

/// Like `run`, but without a consumer thread: once every earlier chunk has
/// reported its output sizes, a chunk's offsets are known and any worker
/// calls `write(i, result, offsets)` for it. Chunks are therefore produced
/// *and* written in parallel while the output stays in order. Returns the
/// final offsets (= output lengths). At most `max_writers` threads are inside
/// `write` at once.
pub fn run_pos<R: Send>(
    n: usize,
    threads: usize,
    window: usize,
    max_writers: usize,
    start: [u64; NOUT],
    work: &(dyn Fn(usize) -> Result<R, WalkErr> + Sync),
    sizes: &(dyn Fn(&R) -> [u64; NOUT] + Sync),
    write: &(dyn Fn(usize, R, [u64; NOUT]) -> Result<(), WalkErr> + Sync),
) -> Result<[u64; NOUT], WalkErr> {
    let sh = Mutex::new(PosShared {
        slots: (0..n).map(|_| None).collect(),
        claimed: 0,
        next: 0,
        offs: start,
        ready: Vec::new(),
        writing: 0,
        err: None,
    });
    let cv = Condvar::new();
    let fail = |e: WalkErr| {
        let mut g = sh.lock().unwrap();
        if g.err.is_none() {
            g.err = Some(e);
        }
        cv.notify_all();
    };
    std::thread::scope(|sc| {
        for _ in 0..threads.clamp(1, n.max(1)) {
            sc.spawn(|| loop {
                let mut g = sh.lock().unwrap();
                if g.err.is_some() {
                    break;
                }
                if g.writing < max_writers {
                    if let Some((i, r, o)) = g.ready.pop() {
                        g.writing += 1;
                        drop(g);
                        let res = write(i, r, o);
                        sh.lock().unwrap().writing -= 1;
                        if let Err(e) = res {
                            fail(e);
                            break;
                        }
                        cv.notify_all();
                        continue;
                    }
                }
                if g.claimed < n && g.claimed < g.next.saturating_add(window) {
                    let i = g.claimed;
                    g.claimed += 1;
                    drop(g);
                    match work(i) {
                        Ok(r) => {
                            let mut g = sh.lock().unwrap();
                            g.slots[i] = Some(r);
                            while g.next < n {
                                let k = g.next;
                                let Some(r) = g.slots[k].take() else { break };
                                let sz = sizes(&r);
                                let o = g.offs;
                                for j in 0..NOUT {
                                    g.offs[j] += sz[j];
                                }
                                g.ready.push((k, r, o));
                                g.next += 1;
                            }
                            cv.notify_all();
                        }
                        Err(e) => {
                            fail(e);
                            break;
                        }
                    }
                    continue;
                }
                if g.next == n && (g.ready.is_empty() || g.writing > 0) {
                    break; // everything is sized; the active writers drain what is left
                }
                drop(cv.wait(g).unwrap());
            });
        }
    });
    let g = sh.into_inner().unwrap();
    match g.err {
        Some(e) => Err(e),
        None => Ok(g.offs),
    }
}
