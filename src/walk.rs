//! The record walker: turns a byte range of the data section into sink calls.
//!
//! Every timestep repeats the same variable ids in the same order, so the hot
//! loop never parses an id: it compares the first 8 bytes of the line against
//! the id it expects next (`ColPat`), finds the end of the value with one
//! 32-byte SIMD compare, and hands the value to the sink. Anything unexpected
//! (timestamps, multi-value lines, long values, reordered or missing ids)
//! drops to `slow_line`, which handles the general case.
use crate::dict::{Dict, FREQ_KIND, NFREQ};
use crate::scan::memchr;
use std::arch::x86_64::*;

pub const SKIP: u32 = u32::MAX;
pub const NONE: usize = NFREQ;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ColPat {
    pub pat: u64,
    pub mask: u64,
    pub len: u32,
    pub outcol: u32,
}
const SENTINEL: ColPat = ColPat {
    pat: 0,
    mask: !0,
    len: 0,
    outcol: SKIP,
};

/// Timestamp of one output row. Written verbatim into the binary format.
#[repr(C)]
#[derive(Clone, Copy, Default, Debug)]
pub struct Stamp {
    pub env: u32,
    pub year: u16,
    /// End minute of the interval, in hundredths of a minute.
    pub min_c: u16,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub kind: u8,
}

pub struct Plan<'a> {
    /// Expected id patterns per frequency, in data order, plus a sentinel.
    /// Index `NONE` holds only the sentinel.
    pub pats: [Vec<ColPat>; NFREQ + 1],
    /// selcount[f][s] = number of selected variables with seq < s.
    pub selcount: [Vec<u32>; NFREQ],
    pub nsel: [usize; NFREQ],
    /// id -> variable index + 1 (0 = unknown).
    pub idmap: &'a [u32],
    /// variable index -> (frequency, position in that frequency's order).
    pub var_fs: Vec<(u8, u32)>,
    pub crlf: bool,
}

impl<'a> Plan<'a> {
    /// `order[f]` lists the variable indices of frequency `f` in the order
    /// their records appear in the data; this is also the output column
    /// order. `sel[i]` says whether variable `i` is wanted. With
    /// `global_cols` the output column of a variable is its index in
    /// `dict.vars`, otherwise its rank among the selected variables of the
    /// same frequency.
    pub fn new(dict: &'a Dict, sel: &[bool], order: &[Vec<u32>; NFREQ], global_cols: bool) -> Plan<'a> {
        let mut pats: [Vec<ColPat>; NFREQ + 1] = Default::default();
        let mut selcount: [Vec<u32>; NFREQ] = Default::default();
        let mut nsel = [0usize; NFREQ];
        let mut var_fs = vec![(0u8, 0u32); dict.vars.len()];
        for f in 0..NFREQ {
            for (seq, &vi) in order[f].iter().enumerate() {
                let i = vi as usize;
                let s = format!("{},", dict.vars[i].id);
                let b = s.as_bytes();
                let (pat, mask) = if b.len() <= 8 {
                    let mut a = [0u8; 8];
                    a[..b.len()].copy_from_slice(b);
                    (u64::from_le_bytes(a), if b.len() == 8 { !0 } else { (1u64 << (8 * b.len())) - 1 })
                } else {
                    (0, !0)
                };
                let outcol = if !sel[i] {
                    SKIP
                } else if global_cols {
                    i as u32
                } else {
                    nsel[f] as u32
                };
                selcount[f].push(nsel[f] as u32);
                if sel[i] {
                    nsel[f] += 1;
                }
                pats[f].push(ColPat {
                    pat,
                    mask,
                    len: b.len() as u32,
                    outcol,
                });
                var_fs[i] = (f as u8, seq as u32);
            }
            selcount[f].push(nsel[f] as u32);
        }
        for p in pats.iter_mut() {
            p.push(SENTINEL);
        }
        Plan {
            pats,
            selcount,
            nsel,
            idmap: &dict.idmap,
            var_fs,
            crlf: dict.crlf,
        }
    }
}

/// A byte range that starts at an environment or timestamp record and ends
/// just before another one (or at the end of the environment).
#[derive(Clone, Copy, Debug)]
pub struct Chunk {
    pub env: u32,
    pub start: usize,
    pub end: usize,
    /// The chunk covers the whole environment.
    pub whole: bool,
}

#[derive(Debug)]
pub enum WalkErr {
    /// Chunked parsing lost context; redo with one chunk per environment.
    NeedSequential,
    Msg(String),
}

pub trait Sink {
    type Cur: Copy;
    fn begin_row(&mut self, f: usize, st: &Stamp);
    /// `next_seq`: how far into the expected sequence the row got.
    fn end_row(&mut self, f: usize, next_seq: usize);
    /// `n` selected columns were absent before the next value.
    fn skip(&mut self, f: usize, n: usize);
    fn put_slow(&mut self, f: usize, outcol: u32, val: &[u8]);
    /// A value that arrived out of the expected order (or twice).
    fn put_ooo(&mut self, f: usize, outcol: u32, val: &[u8]) -> Result<(), WalkErr>;
    fn cursor(&mut self, f: usize) -> Self::Cur;
    fn restore(&mut self, f: usize, cur: Self::Cur);
    /// Hot path. `v` points at `n` (< 32) value bytes; `x` holds the 32 bytes
    /// loaded from `v`.
    unsafe fn put(cur: &mut Self::Cur, outcol: u32, v: *const u8, n: usize, x: __m256i);
}

struct St {
    open: [bool; NFREQ + 1],
    next: [u32; NFREQ + 1],
    stamps: [Stamp; 5],
    seen: [bool; 5],
    cur_f: usize,
    slow: u64,
}

enum Step {
    Next(usize),
    Stop,
}

pub fn walk<S: Sink>(buf: &[u8], plan: &Plan, sink: &mut S, ch: &Chunk) -> Result<u64, WalkErr> {
    let hard_end = ch.end;
    let mut p = ch.start;
    let fast_end = hard_end.min(buf.len().saturating_sub(64));
    let base = buf.as_ptr();
    let eol0 = if plan.crlf { b'\r' } else { b'\n' };
    let eol_len = if plan.crlf { 2 } else { 1 };
    let mut st = St {
        open: [false; NFREQ + 1],
        next: [0; NFREQ + 1],
        stamps: [Stamp::default(); 5],
        seen: [false; 5],
        cur_f: NONE,
        slow: 0,
    };
    unsafe {
        let comma = _mm256_set1_epi8(b',' as i8);
        // Bytes below 0x21 (CR, LF, space) or a comma end the value. A space
        // sends the line to the slow path, which trims it.
        let ctl = _mm256_set1_epi8(0x21);
        loop {
            let f = st.cur_f;
            let pats = plan.pats[f].as_ptr();
            let mut s = st.next[f] as usize;
            let mut cur = sink.cursor(f);
            while p < fast_end {
                let cp = *pats.add(s);
                let w = (base.add(p) as *const u64).read_unaligned();
                if w & cp.mask != cp.pat {
                    break;
                }
                let v = base.add(p + cp.len as usize);
                let x = _mm256_loadu_si256(v as *const __m256i);
                let m = _mm256_movemask_epi8(_mm256_or_si256(_mm256_cmpeq_epi8(x, comma), _mm256_cmpgt_epi8(ctl, x))) as u32;
                let n = m.trailing_zeros() as usize;
                if m == 0 || *v.add(n) != eol0 {
                    break;
                }
                if cp.outcol != SKIP {
                    S::put(&mut cur, cp.outcol, v, n, x);
                }
                s += 1;
                p += cp.len as usize + n + eol_len;
            }
            sink.restore(f, cur);
            st.next[f] = s as u32;
            if p >= hard_end {
                break;
            }
            st.slow += 1;
            match slow_line(buf, plan, sink, ch, &mut st, p)? {
                Step::Next(np) => p = np,
                Step::Stop => break,
            }
        }
    }
    for f in 0..NFREQ {
        if st.open[f] {
            sink.end_row(f, st.next[f] as usize);
        }
    }
    Ok(st.slow)
}

fn field_u32(f: Option<&[u8]>) -> u32 {
    let mut v = 0u32;
    if let Some(b) = f {
        for &c in b {
            if c.is_ascii_digit() {
                v = v.wrapping_mul(10).wrapping_add((c - b'0') as u32);
            }
        }
    }
    v
}

fn malformed(p: usize, what: &str) -> WalkErr {
    WalkErr::Msg(format!("malformed data at byte offset {}: {}", p, what))
}

#[inline(never)]
fn slow_line<S: Sink>(buf: &[u8], plan: &Plan, sink: &mut S, ch: &Chunk, st: &mut St, p: usize) -> Result<Step, WalkErr> {
    let lim = ch.end;
    let e = memchr(b'\n', &buf[p..lim]).map(|i| p + i).unwrap_or(lim);
    let next_p = (e + 1).min(lim);
    let mut line = &buf[p..e];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    if line.is_empty() {
        return Ok(Step::Next(next_p));
    }
    let mut i = 0;
    let mut id = 0u64;
    while i < line.len() && i < 10 && line[i].is_ascii_digit() {
        id = id * 10 + (line[i] - b'0') as u64;
        i += 1;
    }
    if i == 0 || i >= line.len() || line[i] != b',' {
        if line.starts_with(b"End of Data") {
            return Ok(Step::Stop);
        }
        return Err(malformed(p, "expected 'id,value'"));
    }
    let rest = &line[i + 1..];
    if id <= 6 {
        if id <= 1 {
            return Ok(Step::Next(next_p)); // environment record: known from the chunk
        }
        let mut it = rest.split(|&c| c == b',');
        let mut s = Stamp {
            env: ch.env,
            kind: id as u8,
            ..Stamp::default()
        };
        match id {
            2 => {
                it.next();
                s.month = field_u32(it.next()) as u8;
                s.day = field_u32(it.next()) as u8;
                it.next();
                s.hour = field_u32(it.next()) as u8;
                it.next();
                let em = it
                    .next()
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .and_then(|t| t.trim().parse::<f64>().ok())
                    .unwrap_or(60.0);
                s.min_c = (em * 100.0).round().clamp(0.0, 65535.0) as u16;
            }
            3 => {
                it.next();
                s.month = field_u32(it.next()) as u8;
                s.day = field_u32(it.next()) as u8;
            }
            4 => {
                it.next();
                s.month = field_u32(it.next()) as u8;
            }
            5 => {}
            _ => s.year = field_u32(it.next()) as u16,
        }
        for f in 0..NFREQ {
            if FREQ_KIND[f] as u64 == id && st.open[f] {
                sink.end_row(f, st.next[f] as usize);
                st.open[f] = false;
            }
        }
        let k = id as usize - 2;
        st.stamps[k] = s;
        st.seen[k] = true;
        st.cur_f = NONE;
        return Ok(Step::Next(next_p));
    }
    let vi = plan.idmap.get(id as usize).copied().unwrap_or(0);
    if vi == 0 {
        return Err(malformed(p, "variable id is not in the data dictionary"));
    }
    let (f, seq) = plan.var_fs[vi as usize - 1];
    let f = f as usize;
    let outcol = plan.pats[f][seq as usize].outcol;
    let mut val = match rest.iter().position(|&c| c == b',') {
        Some(c) => &rest[..c],
        None => rest,
    };
    while val.first() == Some(&b' ') {
        val = &val[1..];
    }
    while val.last() == Some(&b' ') {
        val = &val[..val.len() - 1];
    }
    if !st.open[f] {
        let k = FREQ_KIND[f] as usize - 2;
        if !st.seen[k] {
            return Err(if ch.whole {
                malformed(p, "value before any timestamp record")
            } else {
                WalkErr::NeedSequential
            });
        }
        sink.begin_row(f, &st.stamps[k]);
        st.open[f] = true;
        st.next[f] = 0;
    }
    if seq < st.next[f] {
        if outcol != SKIP {
            sink.put_ooo(f, outcol, val)?;
        }
    } else {
        let miss = plan.selcount[f][seq as usize] - plan.selcount[f][st.next[f] as usize];
        if miss > 0 {
            sink.skip(f, miss as usize);
        }
        if outcol != SKIP {
            sink.put_slow(f, outcol, val);
        }
        st.next[f] = seq + 1;
    }
    st.cur_f = f;
    Ok(Step::Next(next_p))
}
