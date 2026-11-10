//! Sinks: what happens to each value the walker finds.
use crate::dict::{Dict, FREQ_NAMES, NFREQ};
use crate::walk::{Plan, Sink, Stamp, WalkErr};
use std::arch::x86_64::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub static BAD_VALUE: AtomicBool = AtomicBool::new(false);

pub fn csv_esc(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn push2(b: &mut Vec<u8>, v: u32) {
    b.push(b'0' + (v / 10 % 10) as u8);
    b.push(b'0' + (v % 10) as u8);
}

/// End-of-interval timestamp in EnergyPlus convention (hours run 01..24):
/// `MM/DD HH:MM:SS` for sub-daily data, `MM/DD` daily, `MM` monthly, empty
/// for run period, `YYYY` for annual.
pub fn write_dt(b: &mut Vec<u8>, f: usize, st: &Stamp) {
    match f {
        0..=2 => {
            push2(b, st.month as u32);
            b.push(b'/');
            push2(b, st.day as u32);
            b.push(b' ');
            let (h, m, s) = if f == 2 || st.min_c >= 6000 {
                (st.hour as u32, 0, 0)
            } else {
                ((st.hour as u32).saturating_sub(1), st.min_c as u32 / 100, (st.min_c as u32 % 100) * 60 / 100)
            };
            push2(b, h);
            b.push(b':');
            push2(b, m);
            b.push(b':');
            push2(b, s);
        }
        3 => {
            push2(b, st.month as u32);
            b.push(b'/');
            push2(b, st.day as u32);
        }
        4 => push2(b, st.month as u32),
        5 => {}
        _ => b.extend_from_slice(st.year.to_string().as_bytes()),
    }
}

// ---------------------------------------------------------------- wide text

/// Wide CSV by pass-through: value bytes are copied from the input unparsed.
pub struct WideSink<'a> {
    plan: &'a Plan<'a>,
    env_prefix: &'a [u8],
    pub bufs: [Vec<u8>; NFREQ],
    /// Where the value fields of the open row start in `bufs[f]`.
    row_start: [usize; NFREQ],
    /// Values that arrived out of order in the open row: (column, text).
    ooo: [Vec<(u32, Vec<u8>)>; NFREQ],
}

impl<'a> WideSink<'a> {
    /// `bufs` are (empty) buffers to fill; passing recycled ones avoids
    /// faulting in fresh memory for every chunk.
    pub fn new(plan: &'a Plan<'a>, env_prefix: &'a [u8], bufs: [Vec<u8>; NFREQ]) -> Self {
        WideSink {
            plan,
            env_prefix,
            bufs,
            row_start: [0; NFREQ],
            ooo: Default::default(),
        }
    }
    fn row_reserve(&self, f: usize) -> usize {
        self.plan.nsel[f] * 34 + 64
    }
}

impl<'a> Sink for WideSink<'a> {
    type Cur = *mut u8;
    fn begin_row(&mut self, f: usize, st: &Stamp) {
        if self.plan.nsel[f] == 0 {
            return;
        }
        let need = self.env_prefix.len() + 32 + self.row_reserve(f);
        let b = &mut self.bufs[f];
        b.reserve(need);
        b.extend_from_slice(self.env_prefix);
        write_dt(b, f, st);
        b.push(b',');
        self.row_start[f] = b.len();
    }
    fn end_row(&mut self, f: usize, next_seq: usize) {
        let nsel = self.plan.nsel[f];
        if nsel == 0 {
            return;
        }
        let miss = nsel - self.plan.selcount[f][next_seq] as usize;
        let b = &mut self.bufs[f];
        b.resize(b.len() + miss, b',');
        if !self.ooo[f].is_empty() {
            // Rare: rebuild the row with the late values in their columns.
            let row = b.split_off(self.row_start[f]);
            let mut cells: Vec<&[u8]> = row[..row.len() - 1].split(|&c| c == b',').collect();
            for (col, val) in &self.ooo[f] {
                cells[*col as usize] = val;
            }
            for c in cells {
                b.extend_from_slice(c);
                b.push(b',');
            }
            self.ooo[f].clear();
        }
        *b.last_mut().unwrap() = b'\n';
    }
    fn skip(&mut self, f: usize, n: usize) {
        let b = &mut self.bufs[f];
        b.resize(b.len() + n, b',');
    }
    fn put_slow(&mut self, f: usize, _outcol: u32, val: &[u8]) {
        let need = val.len() + 1 + self.row_reserve(f);
        let b = &mut self.bufs[f];
        b.reserve(need);
        b.extend_from_slice(val);
        b.push(b',');
    }
    fn put_ooo(&mut self, f: usize, outcol: u32, val: &[u8]) -> Result<(), WalkErr> {
        self.ooo[f].push((outcol, val.to_vec()));
        Ok(())
    }
    #[inline(always)]
    fn cursor(&mut self, f: usize) -> *mut u8 {
        if f < NFREQ {
            let b = &mut self.bufs[f];
            unsafe { b.as_mut_ptr().add(b.len()) }
        } else {
            std::ptr::null_mut()
        }
    }
    #[inline(always)]
    fn restore(&mut self, f: usize, cur: *mut u8) {
        if f < NFREQ {
            let b = &mut self.bufs[f];
            unsafe { b.set_len(cur.offset_from(b.as_ptr()) as usize) }
        }
    }
    #[inline(always)]
    unsafe fn put(cur: &mut *mut u8, _outcol: u32, _v: *const u8, n: usize, x: __m256i) {
        _mm256_storeu_si256(*cur as *mut __m256i, x);
        *cur.add(n) = b',';
        *cur = cur.add(n + 1);
    }
}

// ---------------------------------------------------------------- long text

pub struct LongParts {
    arena: Vec<u8>,
    mids: Vec<(u32, u32)>,
    maxmid: usize,
    total_sel: usize,
}

impl LongParts {
    /// Per-variable `Frequency,Key,Variable,Units,` text.
    pub fn new(dict: &Dict, plan: &Plan) -> LongParts {
        let mut arena = Vec::new();
        let mut mids = Vec::new();
        let mut maxmid = 0;
        for v in &dict.vars {
            let s = format!("{},{},{},{},", FREQ_NAMES[v.freq as usize], csv_esc(&v.key), csv_esc(&v.name), csv_esc(&v.units));
            mids.push((arena.len() as u32, s.len() as u32));
            maxmid = maxmid.max(s.len());
            arena.extend_from_slice(s.as_bytes());
        }
        arena.extend_from_slice(&[0u8; 64]);
        LongParts {
            arena,
            mids,
            maxmid,
            total_sel: plan.nsel.iter().sum(),
        }
    }
}

pub struct LongSink<'a> {
    env_prefix: &'a [u8],
    lp: &'a LongParts,
    pub buf: Vec<u8>,
    pre: [Vec<u8>; NFREQ],
    line_max: usize,
}

impl<'a> LongSink<'a> {
    pub fn new(env_prefix: &'a [u8], lp: &'a LongParts, buf: Vec<u8>) -> Self {
        let line_max = env_prefix.len() + 32 + lp.maxmid + 160;
        LongSink {
            env_prefix,
            lp,
            buf,
            pre: Default::default(),
            line_max,
        }
    }
    fn line(&mut self, f: usize, outcol: u32, val: &[u8]) {
        let need = val.len() + self.line_max * (self.lp.total_sel + 1);
        self.buf.reserve(need);
        self.buf.extend_from_slice(&self.pre[f]);
        let (o, l) = self.lp.mids[outcol as usize];
        self.buf.extend_from_slice(&self.lp.arena[o as usize..(o + l) as usize]);
        self.buf.extend_from_slice(val);
        self.buf.push(b'\n');
    }
}

#[derive(Clone, Copy)]
pub struct LongCur {
    out: *mut u8,
    pre: *const u8,
    pre_len: usize,
    mids: *const (u32, u32),
    arena: *const u8,
}

/// Copies `len` bytes in 32-byte blocks; may write up to 31 bytes past `len`.
#[inline(always)]
unsafe fn copy32(dst: *mut u8, src: *const u8, len: usize) {
    let mut i = 0;
    loop {
        _mm256_storeu_si256(dst.add(i) as *mut __m256i, _mm256_loadu_si256(src.add(i) as *const __m256i));
        i += 32;
        if i >= len {
            break;
        }
    }
}

impl<'a> Sink for LongSink<'a> {
    type Cur = LongCur;
    fn begin_row(&mut self, f: usize, st: &Stamp) {
        let p = &mut self.pre[f];
        p.clear();
        p.reserve(self.env_prefix.len() + 96);
        p.extend_from_slice(self.env_prefix);
        write_dt(p, f, st);
        p.push(b',');
        self.buf.reserve(self.line_max * (self.lp.total_sel + 1));
    }
    fn end_row(&mut self, _f: usize, _next_seq: usize) {}
    fn skip(&mut self, _f: usize, _n: usize) {}
    fn put_slow(&mut self, f: usize, outcol: u32, val: &[u8]) {
        self.line(f, outcol, val);
    }
    fn put_ooo(&mut self, f: usize, outcol: u32, val: &[u8]) -> Result<(), WalkErr> {
        self.line(f, outcol, val);
        Ok(())
    }
    #[inline(always)]
    fn cursor(&mut self, f: usize) -> LongCur {
        let pre = if f < NFREQ { &self.pre[f][..] } else { &[][..] };
        LongCur {
            out: unsafe { self.buf.as_mut_ptr().add(self.buf.len()) },
            pre: pre.as_ptr(),
            pre_len: pre.len(),
            mids: self.lp.mids.as_ptr(),
            arena: self.lp.arena.as_ptr(),
        }
    }
    #[inline(always)]
    fn restore(&mut self, _f: usize, cur: LongCur) {
        unsafe { self.buf.set_len(cur.out.offset_from(self.buf.as_ptr()) as usize) }
    }
    #[inline(always)]
    unsafe fn put(cur: &mut LongCur, outcol: u32, _v: *const u8, n: usize, x: __m256i) {
        let mut o = cur.out;
        copy32(o, cur.pre, cur.pre_len);
        o = o.add(cur.pre_len);
        let (off, len) = *cur.mids.add(outcol as usize);
        copy32(o, cur.arena.add(off as usize), len as usize);
        o = o.add(len as usize);
        _mm256_storeu_si256(o as *mut __m256i, x);
        *o.add(n) = b'\n';
        cur.out = o.add(n + 1);
    }
}

// ------------------------------------------------------------------ numeric

#[cold]
fn bad_value() -> f64 {
    BAD_VALUE.store(true, Ordering::Relaxed);
    f64::NAN
}

#[inline(always)]
fn parse_f64(b: &[u8]) -> f64 {
    match fast_float2::parse::<f64, _>(b) {
        Ok(x) => x,
        Err(_) => bad_value(),
    }
}

pub struct NumOut {
    pub stamps: [Vec<Stamp>; NFREQ],
    /// Row-major, `nsel[f]` values per row, NaN where a value is absent.
    pub vals: [Vec<f64>; NFREQ],
}

pub struct NumSink<'a> {
    plan: &'a Plan<'a>,
    pub out: NumOut,
}

impl<'a> NumSink<'a> {
    pub fn new(plan: &'a Plan<'a>) -> Self {
        NumSink {
            plan,
            out: NumOut {
                stamps: Default::default(),
                vals: Default::default(),
            },
        }
    }
    fn row(&mut self, f: usize) -> *mut f64 {
        let n = self.plan.nsel[f];
        let v = &mut self.out.vals[f];
        if n == 0 || v.len() < n {
            return std::ptr::null_mut();
        }
        unsafe { v.as_mut_ptr().add(v.len() - n) }
    }
}

impl<'a> Sink for NumSink<'a> {
    type Cur = *mut f64;
    fn begin_row(&mut self, f: usize, st: &Stamp) {
        let n = self.plan.nsel[f];
        if n == 0 {
            return;
        }
        self.out.stamps[f].push(*st);
        let v = &mut self.out.vals[f];
        v.resize(v.len() + n, f64::NAN);
    }
    fn end_row(&mut self, _f: usize, _next_seq: usize) {}
    fn skip(&mut self, _f: usize, _n: usize) {}
    fn put_slow(&mut self, f: usize, outcol: u32, val: &[u8]) {
        let r = self.row(f);
        unsafe { *r.add(outcol as usize) = parse_f64(val) }
    }
    fn put_ooo(&mut self, f: usize, outcol: u32, val: &[u8]) -> Result<(), WalkErr> {
        self.put_slow(f, outcol, val);
        Ok(())
    }
    #[inline(always)]
    fn cursor(&mut self, f: usize) -> *mut f64 {
        if f < NFREQ {
            self.row(f)
        } else {
            std::ptr::null_mut()
        }
    }
    #[inline(always)]
    fn restore(&mut self, _f: usize, _cur: *mut f64) {}
    #[inline(always)]
    unsafe fn put(cur: &mut *mut f64, outcol: u32, v: *const u8, n: usize, _x: __m256i) {
        *cur.add(outcol as usize) = parse_f64(std::slice::from_raw_parts(v, n));
    }
}
