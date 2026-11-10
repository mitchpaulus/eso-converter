//! Output writers.
use crate::dict::{Dict, FREQ_NAMES, NFREQ};
use crate::pipeline;
use crate::posfile::PosFile;
use crate::sinks::{csv_esc, write_dt, LongParts, LongSink, NumOut, NumSink, WideSink};
use crate::walk::{walk, Chunk, Plan, Stamp, WalkErr};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

pub struct Env {
    pub title: String,
    pub off: usize,
    pub end: usize,
    /// CSV-escaped title followed by a comma.
    pub prefix: Vec<u8>,
}

#[derive(Clone)]
pub enum Target {
    File(PathBuf),
    Stdout,
}

impl Target {
    pub fn open(&self) -> Result<Box<dyn Write>, WalkErr> {
        Ok(match self {
            Target::File(p) => Box::new(File::create(p).map_err(|e| WalkErr::Msg(format!("cannot create {}: {}", p.display(), e)))?),
            Target::Stdout => Box::new(std::io::stdout().lock()),
        })
    }
}

pub struct Job<'a> {
    pub buf: &'a [u8],
    pub dict: &'a Dict,
    pub envs: &'a [Env],
    pub chunks: Vec<Chunk>,
    pub threads: usize,
    /// Threads allowed to write output concurrently.
    pub writers: usize,
    pub sel: &'a [bool],
    /// Variable indices per frequency in data (= column) order.
    pub order: &'a [Vec<u32>; NFREQ],
    pub slow_lines: AtomicU64,
    pub timing: bool,
}

fn io(e: std::io::Error) -> WalkErr {
    WalkErr::Msg(format!("write failed: {}", e))
}

impl<'a> Job<'a> {
    pub fn plan(&self, global: bool) -> Plan<'a> {
        Plan::new(self.dict, self.sel, self.order, global)
    }
    pub fn window(&self) -> usize {
        self.threads * 2
    }
    pub fn selected(&self, f: usize) -> impl Iterator<Item = &crate::dict::Var> + '_ {
        self.order[f].iter().filter(move |&&i| self.sel[i as usize]).map(move |&i| &self.dict.vars[i as usize])
    }
    fn wide_header(&self, f: usize) -> Vec<u8> {
        let mut h = String::from("Environment,Date/Time");
        for v in self.selected(f) {
            h.push(',');
            h.push_str(&csv_esc(&v.label()));
        }
        h.push('\n');
        h.into_bytes()
    }
    fn numeric(&self, plan: &Plan) -> Result<Vec<NumOut>, WalkErr> {
        let mut outs = Vec::with_capacity(self.chunks.len());
        pipeline::run(
            self.chunks.len(),
            self.threads,
            usize::MAX,
            &|i| {
                let mut sink = NumSink::new(plan);
                let slow = walk(self.buf, plan, &mut sink, &self.chunks[i])?;
                self.slow_lines.fetch_add(slow, Ordering::Relaxed);
                Ok(sink.out)
            },
            &mut |_, r| {
                outs.push(r);
                Ok(())
            },
        )?;
        Ok(outs)
    }
}

type Bufs = [Vec<u8>; NFREQ];

/// One CSV per frequency, one row per timestamp, one column per variable.
pub fn write_wide(job: &Job, targets: &[Option<Target>; NFREQ]) -> Result<(), WalkErr> {
    let plan = job.plan(false);
    let pool: Mutex<Vec<Bufs>> = Mutex::new(Vec::new());
    let work = |i: usize| -> Result<Bufs, WalkErr> {
        let ch = &job.chunks[i];
        let bufs = pool.lock().unwrap().pop().unwrap_or_default();
        let mut sink = WideSink::new(&plan, &job.envs[ch.env as usize].prefix, bufs);
        let slow = walk(job.buf, &plan, &mut sink, ch)?;
        job.slow_lines.fetch_add(slow, Ordering::Relaxed);
        Ok(sink.bufs)
    };
    let recycle = |mut b: Bufs| {
        for v in b.iter_mut() {
            v.clear();
        }
        pool.lock().unwrap().push(b);
    };
    if targets.iter().flatten().any(|t| matches!(t, Target::Stdout)) {
        let mut ws: [Option<Box<dyn Write>>; NFREQ] = Default::default();
        for f in 0..NFREQ {
            if let (Some(t), true) = (&targets[f], plan.nsel[f] > 0) {
                let mut w = t.open()?;
                w.write_all(&job.wide_header(f)).map_err(io)?;
                ws[f] = Some(w);
            }
        }
        return pipeline::run(job.chunks.len(), job.threads, job.window(), &work, &mut |_, bufs: Bufs| {
            for f in 0..NFREQ {
                if let Some(w) = &mut ws[f] {
                    w.write_all(&bufs[f]).map_err(io)?;
                }
            }
            recycle(bufs);
            Ok(())
        });
    }
    let mut files: [Option<PosFile>; NFREQ] = Default::default();
    let mut start = [0u64; NFREQ];
    for f in 0..NFREQ {
        if let (Some(Target::File(p)), true) = (&targets[f], plan.nsel[f] > 0) {
            let pf = PosFile::create(p).map_err(|e| WalkErr::Msg(format!("cannot create {}: {}", p.display(), e)))?;
            let h = job.wide_header(f);
            pf.write_at(0, &h).map_err(io)?;
            start[f] = h.len() as u64;
            files[f] = Some(pf);
        }
    }
    let end = pipeline::run_pos(
        job.chunks.len(),
        job.threads,
        job.window(),
        job.writers,
        start,
        &work,
        &|b: &Bufs| std::array::from_fn(|f| b[f].len() as u64),
        &|_, b: Bufs, offs| {
            for f in 0..NFREQ {
                if let Some(pf) = &files[f] {
                    pf.write_at(offs[f], &b[f]).map_err(io)?;
                }
            }
            recycle(b);
            Ok(())
        },
    )?;
    for f in 0..NFREQ {
        if let Some(pf) = files[f].take() {
            pf.finish(end[f]).map_err(io)?;
        }
    }
    Ok(())
}

/// One line per value: Environment,Date/Time,Frequency,Key,Variable,Units,Value
pub fn write_long(job: &Job, target: &Target) -> Result<(), WalkErr> {
    const HEADER: &[u8] = b"Environment,Date/Time,Frequency,Key,Variable,Units,Value\n";
    let plan = job.plan(true);
    let lp = LongParts::new(job.dict, &plan);
    let pool: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
    let work = |i: usize| -> Result<Vec<u8>, WalkErr> {
        let ch = &job.chunks[i];
        let buf = pool.lock().unwrap().pop().unwrap_or_default();
        let mut sink = LongSink::new(&job.envs[ch.env as usize].prefix, &lp, buf);
        let slow = walk(job.buf, &plan, &mut sink, ch)?;
        job.slow_lines.fetch_add(slow, Ordering::Relaxed);
        Ok(sink.buf)
    };
    let recycle = |mut b: Vec<u8>| {
        b.clear();
        pool.lock().unwrap().push(b);
    };
    match target {
        Target::Stdout => {
            let mut w = target.open()?;
            w.write_all(HEADER).map_err(io)?;
            pipeline::run(job.chunks.len(), job.threads, job.window(), &work, &mut |_, b: Vec<u8>| {
                w.write_all(&b).map_err(io)?;
                recycle(b);
                Ok(())
            })
        }
        Target::File(p) => {
            let pf = PosFile::create(p).map_err(|e| WalkErr::Msg(format!("cannot create {}: {}", p.display(), e)))?;
            pf.write_at(0, HEADER).map_err(io)?;
            let mut start = [0u64; NFREQ];
            start[0] = HEADER.len() as u64;
            let end = pipeline::run_pos(
                job.chunks.len(),
                job.threads,
                job.window(),
                job.writers,
                start,
                &work,
                &|b: &Vec<u8>| std::array::from_fn(|f| if f == 0 { b.len() as u64 } else { 0 }),
                &|_, b: Vec<u8>, offs| {
                    pf.write_at(offs[0], &b).map_err(io)?;
                    recycle(b);
                    Ok(())
                },
            )?;
            pf.finish(end[0]).map_err(io)
        }
    }
}

/// Writes a finished in-memory file image in parallel slices.
pub fn write_image(job: &Job, path: &std::path::Path, image: &[u8]) -> Result<(), WalkErr> {
    let pf = PosFile::create(path).map_err(|e| WalkErr::Msg(format!("cannot create {}: {}", path.display(), e)))?;
    const SLICE: usize = 2 << 20;
    let n = image.len().div_ceil(SLICE);
    let err: Mutex<Option<std::io::Error>> = Mutex::new(None);
    pipeline::par_for(n, job.threads.min(job.writers), &|i| {
        let a = i * SLICE;
        let b = (a + SLICE).min(image.len());
        if let Err(e) = pf.write_at(a as u64, &image[a..b]) {
            *err.lock().unwrap() = Some(e);
        }
    });
    if let Some(e) = err.into_inner().unwrap() {
        return Err(io(e));
    }
    pf.finish(image.len() as u64).map_err(io)
}

fn json_str(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
    o
}

struct SendPtr(*mut u8);
unsafe impl Sync for SendPtr {}
unsafe impl Send for SendPtr {}

/// All chunks parsed to numbers, plus where each chunk's rows land.
pub struct Tables {
    pub outs: Vec<NumOut>,
    pub rows: [usize; NFREQ],
    /// row0[i][f]: first row of chunk i in table f.
    row0: Vec<[usize; NFREQ]>,
}

pub fn collect(job: &Job, plan: &Plan) -> Result<Tables, WalkErr> {
    let t0 = std::time::Instant::now();
    let outs = job.numeric(plan)?;
    let mut rows = [0usize; NFREQ];
    let mut row0 = Vec::with_capacity(outs.len());
    for o in &outs {
        row0.push(rows);
        for f in 0..NFREQ {
            rows[f] += o.stamps[f].len();
        }
    }
    if job.timing {
        eprintln!("  parse            {:8.2} ms", t0.elapsed().as_secs_f64() * 1e3);
    }
    Ok(Tables { outs, rows, row0 })
}

/// Transposes table `f` into column-major f64: column j goes to
/// `out + col_off(j)`, `rows[f] * 8` bytes, little-endian.
pub fn fill_columns(job: &Job, t: &Tables, f: usize, ncols: usize, out: &mut [u8], col_off: &(dyn Fn(usize) -> usize + Sync)) {
    if ncols == 0 || t.rows[f] == 0 {
        return;
    }
    for j in 0..ncols {
        assert!(col_off(j) + t.rows[f] * 8 <= out.len());
    }
    let ptr = SendPtr(out.as_mut_ptr());
    let ptr = &ptr;
    pipeline::par_for(t.outs.len(), job.threads, &|i| {
        let o = &t.outs[i];
        let r = o.stamps[f].len();
        if r == 0 {
            return;
        }
        let src = o.vals[f].as_ptr();
        for j in 0..ncols {
            unsafe {
                let dst = ptr.0.add(col_off(j) + t.row0[i][f] * 8) as *mut f64;
                for k in 0..r {
                    dst.add(k).write_unaligned(*src.add(k * ncols + j));
                }
            }
        }
    });
}

/// Single-file binary columnar format; see README for the layout.
pub fn write_bin(job: &Job, path: &std::path::Path) -> Result<(), WalkErr> {
    let plan = job.plan(false);
    let t = collect(job, &plan)?;
    let t1 = std::time::Instant::now();
    let rows = t.rows;
    let align = |x: usize| (x + 63) & !63;
    let ssz = std::mem::size_of::<Stamp>();
    let mut off = 0usize;
    let mut soff = [0usize; NFREQ];
    let mut doff = [0usize; NFREQ];
    let mut tables = Vec::new();
    for f in 0..NFREQ {
        if plan.nsel[f] == 0 {
            continue;
        }
        soff[f] = off;
        off = align(off + rows[f] * ssz);
        doff[f] = off;
        off = align(off + rows[f] * plan.nsel[f] * 8);
        let cols: Vec<String> = job
            .selected(f)
            .map(|v| {
                format!(
                    "{{\"id\":{},\"key\":{},\"name\":{},\"units\":{}}}",
                    v.id,
                    json_str(&v.key),
                    json_str(&v.name),
                    json_str(&v.units)
                )
            })
            .collect();
        tables.push(format!(
            "{{\"frequency\":{},\"rows\":{},\"stamps_offset\":{},\"data_offset\":{},\"columns\":[{}]}}",
            json_str(FREQ_NAMES[f]),
            rows[f],
            soff[f],
            doff[f],
            cols.join(",")
        ));
    }
    let envs: Vec<String> = job.envs.iter().map(|e| json_str(&e.title)).collect();
    let json = format!(
        "{{\"format\":\"esobin\",\"version\":1,\"program\":{},\"byte_order\":\"little\",\"stamp_dtype\":[[\"env\",\"<u4\"],[\"year\",\"<u2\"],[\"minute_x100\",\"<u2\"],[\"month\",\"u1\"],[\"day\",\"u1\"],[\"hour\",\"u1\"],[\"kind\",\"u1\"]],\"environments\":[{}],\"tables\":[{}]}}",
        json_str(&job.dict.version),
        envs.join(","),
        tables.join(",")
    );
    let base = align(16 + json.len());
    let mut out = vec![0u8; base + off];
    out[..8].copy_from_slice(b"ESOBIN01");
    out[8..16].copy_from_slice(&(json.len() as u64).to_le_bytes());
    out[16..16 + json.len()].copy_from_slice(json.as_bytes());
    for f in 0..NFREQ {
        let mut p = base + soff[f];
        for o in &t.outs {
            let n = o.stamps[f].len() * ssz;
            if plan.nsel[f] > 0 && n > 0 {
                // Stamp is repr(C) with no padding, so its bytes are the file layout.
                out[p..p + n].copy_from_slice(unsafe { std::slice::from_raw_parts(o.stamps[f].as_ptr() as *const u8, n) });
                p += n;
            }
        }
        let (d, r) = (base + doff[f], rows[f]);
        fill_columns(job, &t, f, plan.nsel[f], &mut out, &move |j| d + j * r * 8);
    }
    if job.timing {
        eprintln!("  transpose        {:8.2} ms", t1.elapsed().as_secs_f64() * 1e3);
    }
    let t2 = std::time::Instant::now();
    write_image(job, path, &out)?;
    if job.timing {
        eprintln!("  write            {:8.2} ms", t2.elapsed().as_secs_f64() * 1e3);
    }
    Ok(())
}

// ------------------------------------------------------------------ parquet

/// Minimal Thrift compact-protocol writer, enough for Parquet metadata.
struct Thrift {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        *last = id;
        if delta > 0 && delta <= 15 {
            self.b.push((delta as u8) << 4 | ty);
        } else {
            self.b.push(ty);
            self.zigzag(id as i64);
        }
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn string(&mut self, id: i16, s: &str) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s.as_bytes());
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push((n as u8) << 4 | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn struct_field(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn struct_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
}

const PQ_DOUBLE: i32 = 5;
const PQ_BYTE_ARRAY: i32 = 6;

/// One uncompressed PLAIN-encoded Parquet file per frequency, same columns as
/// the wide CSV. All columns are REQUIRED (absent values are NaN), so a
/// column chunk is a page header followed by the raw little-endian values:
/// the transposed table is written straight into place.
pub fn write_parquet(job: &Job, targets: &[Option<Target>; NFREQ]) -> Result<(), WalkErr> {
    let plan = job.plan(false);
    let t = collect(job, &plan)?;
    for f in 0..NFREQ {
        let (Some(Target::File(path)), true) = (&targets[f], plan.nsel[f] > 0) else { continue };
        let t1 = std::time::Instant::now();
        let rows = t.rows[f];
        let ncols = plan.nsel[f];
        if rows * 8 > i32::MAX as usize - 1024 {
            return Err(WalkErr::Msg("table too large for a single Parquet page; use --env or --freq to split".into()));
        }
        // String columns, PLAIN: u32 length + bytes per value.
        let mut env_body = Vec::new();
        let mut dt_body = Vec::new();
        let mut dt = Vec::new();
        for o in &t.outs {
            for st in &o.stamps[f] {
                let title = job.envs[st.env as usize].title.as_bytes();
                env_body.extend_from_slice(&(title.len() as u32).to_le_bytes());
                env_body.extend_from_slice(title);
                dt.clear();
                write_dt(&mut dt, f, st);
                dt_body.extend_from_slice(&(dt.len() as u32).to_le_bytes());
                dt_body.extend_from_slice(&dt);
            }
        }
        let mut names: Vec<String> = vec!["Environment".into(), "Date/Time".into()];
        names.extend(job.selected(f).map(|v| v.label()));
        let total_cols = names.len();
        let body_len = |c: usize| match c {
            0 => env_body.len(),
            1 => dt_body.len(),
            _ => rows * 8,
        };
        // Page headers and layout.
        let mut headers: Vec<Vec<u8>> = Vec::with_capacity(total_cols);
        let mut page_off = Vec::with_capacity(total_cols);
        let mut pos = 4usize;
        if rows > 0 {
            for c in 0..total_cols {
                let mut h = Thrift::new();
                h.i32(1, 0); // DATA_PAGE
                h.i32(2, body_len(c) as i32);
                h.i32(3, body_len(c) as i32);
                h.struct_field(5);
                h.i32(1, rows as i32);
                h.i32(2, 0); // PLAIN
                h.i32(3, 3); // RLE (no levels are stored for REQUIRED columns)
                h.i32(4, 3);
                h.end();
                h.b.push(0);
                page_off.push(pos);
                pos += h.b.len() + body_len(c);
                headers.push(h.b);
            }
        }
        let data_end = pos;
        // Footer.
        let mut m = Thrift::new();
        m.i32(1, 1);
        m.list(2, 12, total_cols + 1);
        m.struct_elem();
        m.string(4, "schema");
        m.i32(5, total_cols as i32);
        m.end();
        for (c, name) in names.iter().enumerate() {
            m.struct_elem();
            m.i32(1, if c < 2 { PQ_BYTE_ARRAY } else { PQ_DOUBLE });
            m.i32(3, 0); // REQUIRED
            m.string(4, name);
            if c < 2 {
                m.i32(6, 0); // UTF8
            }
            m.end();
        }
        m.i64(3, rows as i64);
        if rows > 0 {
            m.list(4, 12, 1);
            m.struct_elem();
            m.list(1, 12, total_cols);
            for (c, name) in names.iter().enumerate() {
                let size = (headers[c].len() + body_len(c)) as i64;
                m.struct_elem();
                m.i64(2, page_off[c] as i64);
                m.struct_field(3);
                m.i32(1, if c < 2 { PQ_BYTE_ARRAY } else { PQ_DOUBLE });
                m.list(2, 5, 1);
                m.zigzag(0); // PLAIN
                m.list(3, 8, 1);
                m.varint(name.len() as u64);
                m.b.extend_from_slice(name.as_bytes());
                m.i32(4, 0); // UNCOMPRESSED
                m.i64(5, rows as i64);
                m.i64(6, size);
                m.i64(7, size);
                m.i64(9, page_off[c] as i64);
                m.end();
                m.end();
            }
            m.i64(2, (data_end - 4) as i64);
            m.i64(3, rows as i64);
            m.end();
        } else {
            m.list(4, 12, 0);
        }
        m.string(6, concat!("esoconv ", env!("CARGO_PKG_VERSION")));
        m.b.push(0);
        let footer = m.b;

        let mut out = vec![0u8; data_end + footer.len() + 8];
        out[..4].copy_from_slice(b"PAR1");
        let mut body_off = vec![0usize; total_cols];
        for c in 0..headers.len() {
            let p = page_off[c];
            out[p..p + headers[c].len()].copy_from_slice(&headers[c]);
            body_off[c] = p + headers[c].len();
        }
        if rows > 0 {
            out[body_off[0]..body_off[0] + env_body.len()].copy_from_slice(&env_body);
            out[body_off[1]..body_off[1] + dt_body.len()].copy_from_slice(&dt_body);
            let bo = &body_off;
            fill_columns(job, &t, f, ncols, &mut out, &move |j| bo[j + 2]);
        }
        out[data_end..data_end + footer.len()].copy_from_slice(&footer);
        out[data_end + footer.len()..data_end + footer.len() + 4].copy_from_slice(&(footer.len() as u32).to_le_bytes());
        out[data_end + footer.len() + 4..].copy_from_slice(b"PAR1");
        let t2 = std::time::Instant::now();
        write_image(job, path, &out)?;
        if job.timing {
            eprintln!(
                "  {:9} build {:8.2} ms, write {:8.2} ms",
                FREQ_NAMES[f],
                (t2 - t1).as_secs_f64() * 1e3,
                t2.elapsed().as_secs_f64() * 1e3
            );
        }
    }
    Ok(())
}
