//! Direct SQLite database writer.
//!
//! Going through the SQLite library costs roughly a microsecond per inserted
//! row; an .eso file has tens of millions of values. Instead the database
//! file is produced directly: the `data` table is a WITHOUT ROWID table,
//! i.e. one index B-tree sorted by (variable_id, time_id). That is exactly
//! column-major order, so its leaf pages are encoded in parallel, one group
//! of columns per task, and stitched together with interior pages built
//! bottom-up. Leaf pages hold no page numbers, so they are position
//! independent and can be streamed to disk as they are produced.
use crate::dict::{FREQ_NAMES, NFREQ};
use crate::out::{collect, fill_columns, Job};
use crate::pipeline;
use crate::posfile::PosFile;
use crate::sinks::write_dt;
use crate::walk::WalkErr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

const PS: usize = 4096;
/// The page containing the lock byte (offset 1 GiB) must stay unused.
const LOCK_PAGE: u32 = (0x4000_0000 / PS) as u32 + 1;
/// Children per interior page (cells are at most 27 bytes with pointer).
const FANOUT: usize = 140;

/// Page number of the `li`-th page written (0-based), skipping the lock page.
fn pgno(li: usize) -> u32 {
    let p = li as u32 + 1;
    if p >= LOCK_PAGE {
        p + 1
    } else {
        p
    }
}

fn put_varint(b: &mut Vec<u8>, v: u64) {
    assert!(v < 1 << 56);
    let mut tmp = [0u8; 8];
    let mut n = 0;
    let mut x = v;
    loop {
        tmp[n] = (x & 0x7f) as u8;
        n += 1;
        x >>= 7;
        if x == 0 {
            break;
        }
    }
    for i in (0..n).rev() {
        b.push(tmp[i] | if i > 0 { 0x80 } else { 0 });
    }
}

/// Serial type and big-endian byte count for an integer.
#[inline(always)]
fn int_ser(v: i64) -> (u8, usize) {
    match v {
        0 => (8, 0),
        1 => (9, 0),
        -128..=127 => (1, 1),
        -32768..=32767 => (2, 2),
        -8388608..=8388607 => (3, 3),
        -2147483648..=2147483647 => (4, 4),
        -140737488355328..=140737488355327 => (5, 6),
        _ => (6, 8),
    }
}

/// Record (row) builder.
#[derive(Default)]
struct Rec {
    types: Vec<u8>,
    body: Vec<u8>,
}

impl Rec {
    fn null(mut self) -> Self {
        self.types.push(0);
        self
    }
    fn int(mut self, v: i64) -> Self {
        let (st, n) = int_ser(v);
        self.types.push(st);
        self.body.extend_from_slice(&v.to_be_bytes()[8 - n..]);
        self
    }
    /// SQLite stores integral REALs as integers and converts on read.
    fn real(mut self, v: f64) -> Self {
        let iv = v as i32;
        if iv as f64 == v && (iv != 0 || v.is_sign_positive()) {
            return self.int(iv as i64);
        }
        self.types.push(7);
        self.body.extend_from_slice(&v.to_bits().to_be_bytes());
        self
    }
    fn text(mut self, s: &str) -> Self {
        put_varint(&mut self.types, 13 + 2 * s.len() as u64);
        self.body.extend_from_slice(s.as_bytes());
        self
    }
    fn payload(&self) -> Vec<u8> {
        let mut p = Vec::with_capacity(self.types.len() + self.body.len() + 2);
        let h = self.types.len() + 1;
        put_varint(&mut p, if h < 128 { h } else { h + 1 } as u64);
        p.extend_from_slice(&self.types);
        p.extend_from_slice(&self.body);
        p
    }
}

/// Writes cells into one B-tree page. `h` is the offset of the page header
/// (100 on page 1, else 0).
struct PageW<'a> {
    b: &'a mut [u8],
    h: usize,
    hdr: usize,
    ncell: usize,
    top: usize,
}

impl<'a> PageW<'a> {
    fn new(b: &'a mut [u8], h: usize, kind: u8) -> Self {
        b[h] = kind;
        let hdr = if kind == 0x02 || kind == 0x05 { 12 } else { 8 };
        PageW { b, h, hdr, ncell: 0, top: PS }
    }
    fn add(&mut self, cell: &[u8]) -> bool {
        if self.h + self.hdr + 2 * (self.ncell + 1) + cell.len() > self.top {
            return false;
        }
        self.top -= cell.len();
        self.b[self.top..self.top + cell.len()].copy_from_slice(cell);
        let p = self.h + self.hdr + 2 * self.ncell;
        self.b[p..p + 2].copy_from_slice(&(self.top as u16).to_be_bytes());
        self.ncell += 1;
        true
    }
    fn finish(self, rightmost: Option<u32>) {
        let h = self.h;
        self.b[h + 3..h + 5].copy_from_slice(&(self.ncell as u16).to_be_bytes());
        self.b[h + 5..h + 7].copy_from_slice(&(self.top as u16).to_be_bytes());
        if let Some(r) = rightmost {
            self.b[h + 8..h + 12].copy_from_slice(&r.to_be_bytes());
        }
    }
}

/// Splits `n` children into runs of at most FANOUT, never leaving a run of 1.
fn runs(n: usize) -> Vec<usize> {
    let mut r = vec![FANOUT; n / FANOUT];
    if !n.is_multiple_of(FANOUT) {
        r.push(n % FANOUT);
    }
    let k = r.len();
    if k > 1 && r[k - 1] == 1 {
        r[k - 2] -= 1;
        r[k - 1] += 1;
    }
    r
}

/// Appends a rowid-table B-tree to `buf` (whose first page is logical page
/// `li0`). Rows must come in ascending rowid order. Returns the root page.
fn build_table(buf: &mut Vec<u8>, li0: usize, rows: impl Iterator<Item = (i64, Vec<u8>)>) -> u32 {
    let li = |buf: &Vec<u8>| li0 + buf.len() / PS;
    let mut nodes: Vec<(usize, i64)> = Vec::new(); // (logical page, max rowid)
    let mut cur: Option<(usize, usize, usize, i64)> = None; // (start, ncell, top, last rowid)
    let mut cell = Vec::new();
    let close = |buf: &mut Vec<u8>, c: (usize, usize, usize, i64)| {
        let mut p = PageW::new(&mut buf[c.0..c.0 + PS], 0, 0x0D);
        p.ncell = c.1;
        p.top = c.2;
        p.finish(None);
    };
    for (rowid, payload) in rows {
        assert!(payload.len() <= PS - 35, "row too large for a page");
        cell.clear();
        put_varint(&mut cell, payload.len() as u64);
        put_varint(&mut cell, rowid as u64);
        cell.extend_from_slice(&payload);
        loop {
            if cur.is_none() {
                let start = buf.len();
                buf.resize(start + PS, 0);
                cur = Some((start, 0, PS, 0));
            }
            let c = cur.as_mut().unwrap();
            let mut p = PageW::new(&mut buf[c.0..c.0 + PS], 0, 0x0D);
            p.ncell = c.1;
            p.top = c.2;
            if p.add(&cell) {
                *c = (c.0, p.ncell, p.top, rowid);
                break;
            }
            let done = cur.take().unwrap();
            nodes.push((li0 + done.0 / PS, done.3));
            close(buf, done);
        }
    }
    match cur.take() {
        Some(done) => {
            nodes.push((li0 + done.0 / PS, done.3));
            close(buf, done);
        }
        None if nodes.is_empty() => {
            let start = buf.len();
            buf.resize(start + PS, 0);
            nodes.push((li0 + start / PS, 0));
            close(buf, (start, 0, PS, 0));
        }
        None => {}
    }
    while nodes.len() > 1 {
        let mut next = Vec::new();
        let mut idx = 0;
        for size in runs(nodes.len()) {
            let me = li(buf);
            let start = buf.len();
            buf.resize(start + PS, 0);
            let mut p = PageW::new(&mut buf[start..start + PS], 0, 0x05);
            for &(child, key) in &nodes[idx..idx + size - 1] {
                cell.clear();
                cell.extend_from_slice(&pgno(child).to_be_bytes());
                put_varint(&mut cell, key as u64);
                assert!(p.add(&cell));
            }
            let last = nodes[idx + size - 1];
            p.finish(Some(pgno(last.0)));
            next.push((me, last.1));
            idx += size;
        }
        nodes = next;
    }
    pgno(nodes[0].0)
}

/// A divider key of the data B-tree: [payload length, payload...].
type Div = [u8; 24];

struct GroupOut {
    pages: Vec<u8>,
    divs: Vec<Div>,
}

#[derive(Clone, Copy)]
struct ColD {
    id: u32,
    data: *const f64,
    rows: usize,
    /// time_id of row 0.
    t0: u32,
}
unsafe impl Sync for ColD {}
unsafe impl Send for ColD {}

/// Encodes the (variable_id, time_id, value) records of `cols` into index
/// leaf pages. With `lead`, the first record becomes the divider between the
/// previous group's last leaf and this group's first leaf. The group must
/// hold at least two records when `lead` is set.
fn encode_group(cols: &[ColD], lead: bool, est_records: usize) -> GroupOut {
    let mut pages: Vec<u8> = Vec::with_capacity((est_records * 24 / PS + 2) * PS);
    let mut divs: Vec<Div> = Vec::new();
    // Current page state; `open` is false until a record needs a page.
    let (mut start, mut ncell, mut top, mut open) = (0usize, 0usize, PS, false);
    let mut want_div = lead;
    let mut last_was_div = false;
    let mut cell = [0u8; 32];
    let finish = |pages: &mut Vec<u8>, start: usize, ncell: usize, top: usize| {
        let b = &mut pages[start..start + PS];
        b[0] = 0x0A;
        b[3..5].copy_from_slice(&(ncell as u16).to_be_bytes());
        b[5..7].copy_from_slice(&(top as u16).to_be_bytes());
    };
    for c in cols {
        let (vst, vn) = int_ser(c.id as i64);
        let vbytes = (c.id as u64).to_be_bytes();
        for r in 0..c.rows {
            let x = unsafe { *c.data.add(r) };
            if x.is_nan() {
                continue;
            }
            // cell = [payload len][4][st var][st time][st value][var][time][value]
            let t = c.t0 as i64 + r as i64;
            let (tst, tn) = int_ser(t);
            let mut n = 5;
            cell[5..5 + vn].copy_from_slice(&vbytes[8 - vn..]);
            n += vn;
            cell[n..n + tn].copy_from_slice(&t.to_be_bytes()[8 - tn..]);
            n += tn;
            let iv = x as i32;
            let xst = if iv as f64 == x && (iv != 0 || x.is_sign_positive()) {
                let (st, k) = int_ser(iv as i64);
                cell[n..n + k].copy_from_slice(&(iv as i64).to_be_bytes()[8 - k..]);
                n += k;
                st
            } else {
                cell[n..n + 8].copy_from_slice(&x.to_bits().to_be_bytes());
                n += 8;
                7
            };
            cell[0] = (n - 1) as u8;
            cell[1] = 4;
            cell[2] = vst;
            cell[3] = tst;
            cell[4] = xst;
            if open && 8 + 2 * (ncell + 1) + n > top {
                finish(&mut pages, start, ncell, top);
                open = false;
                want_div = true;
            }
            if want_div {
                let mut d = [0u8; 24];
                d[..n].copy_from_slice(&cell[..n]);
                divs.push(d);
                want_div = false;
                last_was_div = true;
                continue;
            }
            if !open {
                start = pages.len();
                pages.resize(start + PS, 0);
                ncell = 0;
                top = PS;
                open = true;
            }
            top -= n;
            pages[start + top..start + top + n].copy_from_slice(&cell[..n]);
            let p = start + 8 + 2 * ncell;
            pages[p..p + 2].copy_from_slice(&(top as u16).to_be_bytes());
            ncell += 1;
            last_was_div = false;
        }
    }
    if open {
        finish(&mut pages, start, ncell, top);
    }
    if last_was_div && !pages.is_empty() {
        // The final record became a divider with no leaf after it. Use the
        // last record of the previous (full) leaf as the divider instead and
        // give the final record a leaf of its own.
        let fin = divs.pop().unwrap();
        let prev = pages.len() - PS;
        let b = &mut pages[prev..prev + PS];
        let nc = u16::from_be_bytes([b[3], b[4]]) as usize;
        let ptr = 8 + 2 * (nc - 1);
        let off = u16::from_be_bytes([b[ptr], b[ptr + 1]]) as usize;
        let len = b[off] as usize + 1;
        let mut d = [0u8; 24];
        d[..len].copy_from_slice(&b[off..off + len]);
        // The popped cell is the lowest one on the page, so the content area simply shrinks.
        debug_assert_eq!(off, u16::from_be_bytes([b[5], b[6]]) as usize);
        b[off..off + len].fill(0);
        b[ptr..ptr + 2].fill(0);
        b[3..5].copy_from_slice(&((nc - 1) as u16).to_be_bytes());
        b[5..7].copy_from_slice(&((off + len) as u16).to_be_bytes());
        divs.push(d);
        let s = pages.len();
        pages.resize(s + PS, 0);
        let n = fin[0] as usize + 1;
        pages[s + PS - n..s + PS].copy_from_slice(&fin[..n]);
        pages[s + 8..s + 10].copy_from_slice(&((PS - n) as u16).to_be_bytes());
        finish(&mut pages, s, 1, PS - n);
    }
    GroupOut { pages, divs }
}

/// Writes whole pages starting at logical page `li`, leaving the lock page
/// empty.
fn write_pages(pf: &PosFile, mut li: usize, mut pages: &[u8]) -> std::io::Result<()> {
    while !pages.is_empty() {
        let p = pgno(li);
        let n = if p < LOCK_PAGE {
            ((LOCK_PAGE - p) as usize).min(pages.len() / PS)
        } else {
            pages.len() / PS
        };
        pf.write_at((p as u64 - 1) * PS as u64, &pages[..n * PS])?;
        li += n;
        pages = &pages[n * PS..];
    }
    Ok(())
}

const SCHEMA: [(&str, &str, &str); 5] = [
    ("table", "environments", "CREATE TABLE environments(id INTEGER PRIMARY KEY, title TEXT NOT NULL)"),
    ("table", "variables", "CREATE TABLE variables(id INTEGER PRIMARY KEY, frequency TEXT NOT NULL, key TEXT NOT NULL, name TEXT NOT NULL, units TEXT NOT NULL)"),
    ("table", "times", "CREATE TABLE times(id INTEGER PRIMARY KEY, environment_id INTEGER NOT NULL, frequency TEXT NOT NULL, datetime TEXT NOT NULL, month INTEGER, day INTEGER, hour INTEGER, minute REAL)"),
    ("table", "data", "CREATE TABLE data(variable_id INTEGER NOT NULL, time_id INTEGER NOT NULL, value REAL, PRIMARY KEY(variable_id, time_id)) WITHOUT ROWID"),
    ("view", "long", "CREATE VIEW long AS SELECT e.title AS environment, t.datetime, v.frequency, v.key, v.name, v.units, d.value FROM data d JOIN variables v ON v.id=d.variable_id JOIN times t ON t.id=d.time_id JOIN environments e ON e.id=t.environment_id"),
];

pub fn write_sqlite(job: &Job, path: &Path) -> Result<(), WalkErr> {
    let io = |e: std::io::Error| WalkErr::Msg(format!("write failed: {}", e));
    let plan = job.plan(false);
    let mut t = collect(job, &plan)?;
    let t1 = std::time::Instant::now();

    // Column-major copies of every table; the row-major chunks are then freed.
    let mut colmaj: [Vec<f64>; NFREQ] = Default::default();
    for f in 0..NFREQ {
        let (rows, ncols) = (t.rows[f], plan.nsel[f]);
        colmaj[f] = vec![0f64; rows * ncols];
        let bytes = unsafe { std::slice::from_raw_parts_mut(colmaj[f].as_mut_ptr() as *mut u8, rows * ncols * 8) };
        fill_columns(job, &t, f, ncols, bytes, &move |j| j * rows * 8);
        for o in t.outs.iter_mut() {
            o.vals[f] = Vec::new();
        }
    }
    let mut tbase = [0u32; NFREQ];
    let mut total_times = 0usize;
    for f in 0..NFREQ {
        tbase[f] = total_times as u32 + 1;
        if plan.nsel[f] > 0 {
            total_times += t.rows[f];
        }
    }
    if total_times > 2_000_000_000 {
        return Err(WalkErr::Msg("too many timestamps for the SQLite writer".into()));
    }
    // Columns in variable-id order = primary key order.
    let mut cols: Vec<ColD> = Vec::new();
    for f in 0..NFREQ {
        for (j, v) in job.selected(f).enumerate() {
            cols.push(ColD {
                id: v.id,
                data: unsafe { colmaj[f].as_ptr().add(j * t.rows[f]) },
                rows: t.rows[f],
                t0: tbase[f],
            });
        }
    }
    cols.sort_by_key(|c| c.id);
    let counts: Vec<AtomicUsize> = cols.iter().map(|_| AtomicUsize::new(0)).collect();
    pipeline::par_for(cols.len(), job.threads, &|i| {
        let c = &cols[i];
        let s = unsafe { std::slice::from_raw_parts(c.data, c.rows) };
        counts[i].store(s.iter().filter(|x| !x.is_nan()).count(), Ordering::Relaxed);
    });
    let counts: Vec<usize> = counts.iter().map(|c| c.load(Ordering::Relaxed)).collect();
    let total: usize = counts.iter().sum();
    // Contiguous groups of columns, each with at least two records.
    let target = (total / (job.threads * 4).max(1)).max(2);
    let mut groups: Vec<(usize, usize, usize)> = Vec::new(); // (first col, end col, records)
    let (mut g0, mut acc) = (0usize, 0usize);
    for (i, &n) in counts.iter().enumerate() {
        acc += n;
        if acc >= target {
            groups.push((g0, i + 1, acc));
            g0 = i + 1;
            acc = 0;
        }
    }
    if g0 < cols.len() || groups.is_empty() {
        match groups.last_mut() {
            Some(last) if acc < 2 => {
                last.1 = cols.len();
                last.2 += acc;
            }
            _ => groups.push((g0, cols.len(), acc)),
        }
    }

    // Small tables first (page 1 is filled in last).
    let mut head = vec![0u8; PS];
    let root_env = build_table(
        &mut head,
        0,
        job.envs.iter().enumerate().map(|(i, e)| (i as i64 + 1, Rec::default().null().text(&e.title).payload())),
    );
    let mut vars: Vec<&crate::dict::Var> = (0..NFREQ).flat_map(|f| job.selected(f)).collect();
    vars.sort_by_key(|v| v.id);
    let root_var = build_table(
        &mut head,
        0,
        vars.iter().map(|v| {
            (
                v.id as i64,
                Rec::default().null().text(FREQ_NAMES[v.freq as usize]).text(&v.key).text(&v.name).text(&v.units).payload(),
            )
        }),
    );
    let mut dt = Vec::new();
    let mut time_rows = Vec::with_capacity(total_times);
    for f in 0..NFREQ {
        if plan.nsel[f] == 0 {
            continue;
        }
        let mut id = tbase[f] as i64;
        for o in &t.outs {
            for st in &o.stamps[f] {
                dt.clear();
                write_dt(&mut dt, f, st);
                let mut r = Rec::default().null().int(st.env as i64 + 1).text(FREQ_NAMES[f]).text(std::str::from_utf8(&dt).unwrap());
                r = if f <= 3 {
                    r.int(st.month as i64).int(st.day as i64)
                } else if f == 4 {
                    r.int(st.month as i64).null()
                } else {
                    r.null().null()
                };
                r = if f <= 2 { r.int(st.hour as i64).real(st.min_c as f64 / 100.0) } else { r.null().null() };
                time_rows.push((id, r.payload()));
                id += 1;
            }
        }
    }
    let root_time = build_table(&mut head, 0, time_rows.into_iter());
    if job.timing {
        eprintln!("  transpose+tables {:8.2} ms", t1.elapsed().as_secs_f64() * 1e3);
    }
    let t2 = std::time::Instant::now();

    let pf = PosFile::create(path).map_err(|e| WalkErr::Msg(format!("cannot create {}: {}", path.display(), e)))?;
    write_pages(&pf, 0, &head).map_err(io)?;
    // Data leaves: encoded and written in parallel. A group's position is
    // known once all earlier groups have reported their page counts.
    let cols_ref = &cols;
    let groups_ref = &groups;
    let metas: Mutex<Vec<(usize, usize, Vec<Div>)>> = Mutex::new(Vec::new()); // (first logical page, pages, dividers)
    let mut start = [0u64; pipeline::NOUT];
    start[0] = (head.len() / PS) as u64;
    let end = pipeline::run_pos(
        groups.len(),
        job.threads,
        job.threads * 2,
        job.writers,
        start,
        &|g| {
            let (a, b, n) = groups_ref[g];
            Ok((g, encode_group(&cols_ref[a..b], g > 0, n)))
        },
        &|r: &(usize, GroupOut)| std::array::from_fn(|k| if k == 0 { (r.1.pages.len() / PS) as u64 } else { 0 }),
        &|_, (g, go): (usize, GroupOut), offs| {
            let np = go.pages.len() / PS;
            assert!(np == 0 && go.divs.is_empty() || go.divs.len() == np - 1 + (g > 0) as usize, "leaf/divider mismatch");
            write_pages(&pf, offs[0] as usize, &go.pages).map_err(io)?;
            metas.lock().unwrap().push((offs[0] as usize, np, go.divs));
            Ok(())
        },
    )?;
    let mut metas = metas.into_inner().unwrap();
    metas.sort_by_key(|m| m.0);
    let mut nodes: Vec<usize> = Vec::new();
    let mut divs: Vec<Div> = Vec::new();
    for (li, np, d) in metas {
        nodes.extend(li..li + np);
        divs.extend_from_slice(&d);
    }
    let tail_li0 = end[0] as usize;
    let mut tail: Vec<u8> = Vec::new();
    if nodes.is_empty() {
        tail.resize(PS, 0);
        PageW::new(&mut tail[..PS], 0, 0x0A).finish(None);
        nodes.push(tail_li0);
    }
    assert_eq!(divs.len(), nodes.len() - 1);
    let mut cell = Vec::new();
    while nodes.len() > 1 {
        let (mut next_nodes, mut next_divs) = (Vec::new(), Vec::new());
        let mut idx = 0;
        let rs = runs(nodes.len());
        for (ri, &size) in rs.iter().enumerate() {
            let s = tail.len();
            tail.resize(s + PS, 0);
            let mut p = PageW::new(&mut tail[s..s + PS], 0, 0x02);
            for i in idx..idx + size - 1 {
                cell.clear();
                cell.extend_from_slice(&pgno(nodes[i]).to_be_bytes());
                cell.extend_from_slice(&divs[i][..divs[i][0] as usize + 1]);
                assert!(p.add(&cell));
            }
            p.finish(Some(pgno(nodes[idx + size - 1])));
            next_nodes.push(tail_li0 + s / PS);
            if ri + 1 < rs.len() {
                next_divs.push(divs[idx + size - 1]);
            }
            idx += size;
        }
        nodes = next_nodes;
        divs = next_divs;
    }
    let root_data = pgno(nodes[0]);
    write_pages(&pf, tail_li0, &tail).map_err(io)?;
    let last_li = tail_li0 + tail.len() / PS - 1;
    let npages = pgno(last_li);

    // Page 1: file header + schema table.
    let mut p1 = vec![0u8; PS];
    p1[..16].copy_from_slice(b"SQLite format 3\0");
    p1[16..18].copy_from_slice(&(PS as u16).to_be_bytes());
    p1[18] = 1; // file format write version (rollback journal)
    p1[19] = 1;
    p1[21] = 64;
    p1[22] = 32;
    p1[23] = 32;
    p1[24..28].copy_from_slice(&1u32.to_be_bytes()); // change counter
    p1[28..32].copy_from_slice(&npages.to_be_bytes());
    p1[40..44].copy_from_slice(&1u32.to_be_bytes()); // schema cookie
    p1[44..48].copy_from_slice(&4u32.to_be_bytes()); // schema format
    p1[56..60].copy_from_slice(&1u32.to_be_bytes()); // UTF-8
    p1[92..96].copy_from_slice(&1u32.to_be_bytes()); // version-valid-for
    p1[96..100].copy_from_slice(&3_045_000u32.to_be_bytes());
    let roots = [root_env, root_var, root_time, root_data, 0];
    let mut pw = PageW::new(&mut p1, 100, 0x0D);
    for (i, (kind, name, sql)) in SCHEMA.iter().enumerate() {
        let payload = Rec::default().text(kind).text(name).text(name).int(roots[i] as i64).text(sql).payload();
        let mut cell = Vec::new();
        put_varint(&mut cell, payload.len() as u64);
        put_varint(&mut cell, i as u64 + 1);
        cell.extend_from_slice(&payload);
        assert!(pw.add(&cell));
    }
    pw.finish(None);
    pf.write_at(0, &p1).map_err(io)?;
    pf.finish(npages as u64 * PS as u64).map_err(io)?;
    if job.timing {
        eprintln!("  encode+write     {:8.2} ms  ({} records, {} pages)", t2.elapsed().as_secs_f64() * 1e3, total, npages);
    }
    Ok(())
}
