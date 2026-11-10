#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi1")))]
compile_error!("esoconv needs AVX2/BMI: build with RUSTFLAGS=\"-C target-cpu=x86-64-v3\" (the repo's .cargo/config.toml sets this)");

mod dict;
mod out;
mod pipeline;
mod posfile;
mod scan;
mod sinks;
mod sqlite;
mod walk;

use dict::{Dict, FREQ_NAMES, NFREQ};
use out::{Env, Job, Target};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use walk::{Chunk, WalkErr};

const USAGE: &str = "\
esoconv - fast EnergyPlus .eso converter

USAGE: esoconv [OPTIONS] <input.eso>

  -f, --format <FMT>   wide (default) | long | parquet | sqlite | bin
  -o, --output <PATH>  output file, or '-' for stdout (CSV formats).
                       Default: input path with the extension replaced.
                       'wide' and 'parquet' write one file per reporting
                       frequency (<base>.hourly.csv, ...) unless a single --freq and an
                       explicit --output are given.
  -e, --env <SEL>      only this environment / run period: 1-based index or
                       case-insensitive part of its title. Repeatable.
      --freq <F>       only this reporting frequency (detailed, timestep,
                       hourly, daily, monthly, runperiod, annual). Repeatable.
  -v, --var <TEXT>     only variables whose 'KEY:Name' contains TEXT
                       (case-insensitive). Repeatable.
  -l, --list           list environments and variable counts, then exit
      --list-vars      list variables, then exit
  -j, --threads <N>    worker threads (default: all cores)
      --timing         print phase timings to stderr
  -h, --help
";

/// Threads writing output at once. Mapped writes (Windows) scale; buffered
/// `pwrite` on Linux serializes on the inode lock, so more writers only add
/// contention.
#[cfg(windows)]
const DEFAULT_WRITERS: usize = usize::MAX;
#[cfg(not(windows))]
const DEFAULT_WRITERS: usize = 2;

struct Args {
    input: String,
    format: String,
    output: Option<String>,
    envs: Vec<String>,
    freqs: Vec<usize>,
    vars: Vec<String>,
    threads: usize,
    chunk: usize,
    writers: usize,
    list: bool,
    list_vars: bool,
    timing: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        input: String::new(),
        format: "wide".into(),
        output: None,
        envs: vec![],
        freqs: vec![],
        vars: vec![],
        threads: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
        chunk: 1 << 20,
        writers: DEFAULT_WRITERS,
        list: false,
        list_vars: false,
        timing: false,
    };
    let mut it = std::env::args().skip(1);
    let mut inputs = vec![];
    while let Some(arg) = it.next() {
        let mut val = |name: &str| it.next().ok_or_else(|| format!("{} needs a value", name));
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{}", USAGE);
                std::process::exit(0);
            }
            "-f" | "--format" => a.format = val("--format")?.to_ascii_lowercase(),
            "-o" | "--output" => a.output = Some(val("--output")?),
            "-e" | "--env" => a.envs.push(val("--env")?),
            "--freq" => {
                for part in val("--freq")?.split(',') {
                    a.freqs.push(dict::parse_freq(part).ok_or_else(|| format!("unknown frequency '{}'", part))?);
                }
            }
            "-v" | "--var" => a.vars.push(val("--var")?.to_lowercase()),
            "-j" | "--threads" => a.threads = val("--threads")?.parse::<usize>().map_err(|_| "bad --threads")?.max(1),
            "--chunk" => a.chunk = val("--chunk")?.parse::<usize>().map_err(|_| "bad --chunk")?.max(4096),
            "--writers" => a.writers = val("--writers")?.parse::<usize>().map_err(|_| "bad --writers")?.max(1),
            "-l" | "--list" => a.list = true,
            "--list-vars" => a.list_vars = true,
            "--timing" => a.timing = true,
            s if s.starts_with('-') && s.len() > 1 => return Err(format!("unknown option '{}'", s)),
            _ => inputs.push(arg),
        }
    }
    if inputs.len() != 1 {
        return Err("expected exactly one input file (see --help)".into());
    }
    a.input = inputs.pop().unwrap();
    Ok(a)
}

fn find_data_end(buf: &[u8], data_start: usize) -> usize {
    let tail = buf.len().saturating_sub(4096).max(data_start);
    let pat = b"\nEnd of Data";
    if let Some(i) = buf[tail..].windows(pat.len()).rposition(|w| w == pat) {
        return tail + i + 1;
    }
    if buf[data_start..].starts_with(&pat[1..]) {
        return data_start;
    }
    // Truncated file (simulation crashed?): use everything up to the last complete line.
    match buf[data_start..].iter().rposition(|&c| c == b'\n') {
        Some(i) => data_start + i + 1,
        None => data_start,
    }
}

/// Offsets of every environment (1) and timestamp (2..=6) record, in order.
fn scan_stamps(buf: &[u8], start: usize, end: usize, threads: usize) -> Vec<usize> {
    if start >= end {
        return vec![];
    }
    let from = start - 1; // the newline that ends the dictionary
    let n = threads.clamp(1, 1 + (end - from) / (4 << 20));
    let step = (end - from).div_ceil(n);
    let parts: Vec<Vec<usize>> = std::thread::scope(|sc| {
        let hs: Vec<_> = (0..n)
            .map(|k| {
                sc.spawn(move || {
                    let mut v = vec![];
                    scan::scan_lines(buf, from + k * step, (from + (k + 1) * step).min(end), b'1', b'6', |o| {
                        v.push(o);
                        true
                    });
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    parts.concat().into_iter().filter(|&o| o < end).collect()
}

/// Learns, per frequency, the order in which variables are written within a
/// row. EnergyPlus keeps that order fixed for the whole file but it is not
/// the dictionary order (meters are sorted differently), so it is read from
/// the first row of each frequency. Variables absent from that row follow in
/// dictionary order.
fn learn_order(buf: &[u8], dict: &Dict, stamps: &[usize], data_end: usize) -> [Vec<u32>; NFREQ] {
    let mut order: [Vec<u32>; NFREQ] = Default::default();
    let mut done = [false; NFREQ];
    for f in 0..NFREQ {
        done[f] = dict.nseq[f] == 0;
    }
    let mut placed = vec![false; dict.vars.len()];
    for kind in 2..=6u8 {
        if (0..NFREQ).all(|f| dict::FREQ_KIND[f] != kind || done[f]) {
            continue;
        }
        let mut budget: usize = 64 << 20;
        for (i, &o) in stamps.iter().enumerate() {
            if buf[o] != b'0' + kind {
                continue;
            }
            let end = stamps.get(i + 1).copied().unwrap_or(data_end);
            let mut found = [false; NFREQ];
            let mut p = o;
            loop {
                p = match scan::memchr(b'\n', &buf[p..end]) {
                    Some(j) => p + j + 1,
                    None => break,
                };
                let mut id = 0usize;
                let mut q = p;
                while q < end && buf[q].is_ascii_digit() && q - p < 10 {
                    id = id * 10 + (buf[q] - b'0') as usize;
                    q += 1;
                }
                let vi = dict.idmap.get(id).copied().unwrap_or(0);
                if vi == 0 {
                    continue;
                }
                let f = dict.vars[vi as usize - 1].freq as usize;
                if !done[f] && !placed[vi as usize - 1] {
                    placed[vi as usize - 1] = true;
                    order[f].push(vi - 1);
                    found[f] = true;
                }
            }
            for f in 0..NFREQ {
                done[f] |= found[f];
            }
            budget = budget.saturating_sub(end - o);
            if budget == 0 || (0..NFREQ).all(|f| dict::FREQ_KIND[f] != kind || done[f]) {
                break;
            }
        }
    }
    for (i, v) in dict.vars.iter().enumerate() {
        if !placed[i] {
            order[v.freq as usize].push(i as u32);
        }
    }
    order
}

fn with_suffix(base: &std::path::Path, suffix: &str) -> PathBuf {
    let mut s = base.as_os_str().to_owned();
    s.push(suffix);
    PathBuf::from(s)
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let t0 = Instant::now();
    let file = std::fs::File::open(&args.input).map_err(|e| format!("cannot open {}: {}", args.input, e))?;
    let len = file.metadata().map_err(|e| e.to_string())?.len();
    if len == 0 {
        return Err("input file is empty".into());
    }
    let map = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| format!("cannot map {}: {}", args.input, e))?;
    let buf: &[u8] = &map;
    let dict: Dict = dict::parse(buf)?;
    let data_end = find_data_end(buf, dict.data_start);
    let t1 = Instant::now();
    let stamps = scan_stamps(buf, dict.data_start, data_end, args.threads);
    let offs: Vec<usize> = stamps.iter().copied().filter(|&o| buf[o] == b'1').collect();
    if dict.data_start < data_end && offs.first() != Some(&dict.data_start) {
        return Err("data section does not start with an environment record".into());
    }
    let envs: Vec<Env> = offs
        .iter()
        .enumerate()
        .map(|(k, &off)| {
            let end = offs.get(k + 1).copied().unwrap_or(data_end);
            let e = scan::memchr(b'\n', &buf[off..end]).map(|i| off + i).unwrap_or(end);
            let line = &buf[(off + 2).min(e)..e];
            let t = line.split(|&c| c == b',').next().unwrap_or(b"");
            let title = String::from_utf8_lossy(t).trim().to_string();
            let prefix = format!("{},", sinks::csv_esc(&title)).into_bytes();
            Env { title, off, end, prefix }
        })
        .collect();
    let order = learn_order(buf, &dict, &stamps, data_end);
    let t2 = Instant::now();

    if args.list || args.list_vars {
        println!("{}", dict.version);
        println!("Environments:");
        for (k, e) in envs.iter().enumerate() {
            println!("  {:3}  {}  ({:.1} MB)", k + 1, e.title, (e.end - e.off) as f64 / 1e6);
        }
        println!("Variables by frequency:");
        for f in 0..NFREQ {
            if dict.nseq[f] > 0 {
                println!("  {:10} {}", FREQ_NAMES[f], dict.nseq[f]);
            }
        }
        if args.list_vars {
            for f in 0..NFREQ {
                for &i in &order[f] {
                    println!("{}\t{}", dict.vars[i as usize].id, dict.vars[i as usize].label());
                }
            }
        }
        return Ok(());
    }

    // Environment selection.
    let mut env_sel = vec![args.envs.is_empty(); envs.len()];
    for spec in &args.envs {
        let mut hit = false;
        if let Ok(n) = spec.parse::<usize>() {
            if n >= 1 && n <= envs.len() {
                env_sel[n - 1] = true;
                hit = true;
            }
        } else {
            let s = spec.to_lowercase();
            for (k, e) in envs.iter().enumerate() {
                if e.title.to_lowercase().contains(&s) {
                    env_sel[k] = true;
                    hit = true;
                }
            }
        }
        if !hit {
            let names: Vec<String> = envs.iter().enumerate().map(|(k, e)| format!("{}: {}", k + 1, e.title)).collect();
            return Err(format!("no environment matches '{}'. Available: {}", spec, names.join("; ")));
        }
    }

    // Variable selection.
    let sel: Vec<bool> = dict
        .vars
        .iter()
        .map(|v| {
            let fok = args.freqs.is_empty() || args.freqs.contains(&(v.freq as usize));
            let vok = args.vars.is_empty() || {
                let l = format!("{}:{}", v.key, v.name).to_lowercase();
                args.vars.iter().any(|s| l.contains(s))
            };
            fok && vok
        })
        .collect();
    let mut nsel = [0usize; NFREQ];
    for (i, v) in dict.vars.iter().enumerate() {
        if sel[i] {
            nsel[v.freq as usize] += 1;
        }
    }
    if nsel.iter().sum::<usize>() == 0 {
        return Err("no variables selected".into());
    }

    // Output targets.
    let stdout = args.output.as_deref() == Some("-");
    let explicit = args.output.is_some() && !stdout;
    let base: PathBuf = match &args.output {
        Some(o) if !stdout => {
            let p = PathBuf::from(o);
            match p.extension().and_then(|e| e.to_str()) {
                Some("csv" | "parquet" | "sqlite" | "db" | "esobin" | "bin") => p.with_extension(""),
                _ => p,
            }
        }
        _ => PathBuf::from(&args.input).with_extension(""),
    };
    let exact = |ext: &str| -> Target {
        if stdout {
            Target::Stdout
        } else if explicit {
            Target::File(PathBuf::from(args.output.as_ref().unwrap()))
        } else {
            Target::File(with_suffix(&base, ext))
        }
    };
    let per_freq = |ext: &str| -> Result<[Option<Target>; NFREQ], String> {
        let active: Vec<usize> = (0..NFREQ).filter(|&f| nsel[f] > 0).collect();
        let mut t: [Option<Target>; NFREQ] = Default::default();
        if stdout {
            if active.len() != 1 {
                return Err("writing 'wide' to stdout needs a single frequency; add --freq".into());
            }
            t[active[0]] = Some(Target::Stdout);
        } else if explicit && args.freqs.len() == 1 {
            t[args.freqs[0]] = Some(Target::File(PathBuf::from(args.output.as_ref().unwrap())));
        } else {
            for f in active {
                t[f] = Some(Target::File(with_suffix(&base, &format!(".{}.{}", FREQ_NAMES[f].to_lowercase(), ext))));
            }
        }
        Ok(t)
    };

    let mut sequential = false;
    let slow;
    loop {
        let mut chunks = vec![];
        for (k, e) in envs.iter().enumerate() {
            if !env_sel[k] {
                continue;
            }
            let first = chunks.len();
            let mut start = e.off;
            if !sequential {
                let i0 = stamps.partition_point(|&o| o <= e.off);
                let i1 = stamps.partition_point(|&o| o < e.end);
                for &s in &stamps[i0..i1] {
                    if s - start >= args.chunk {
                        chunks.push(Chunk {
                            env: k as u32,
                            start,
                            end: s,
                            whole: false,
                        });
                        start = s;
                    }
                }
            }
            chunks.push(Chunk {
                env: k as u32,
                start,
                end: e.end,
                whole: chunks.len() == first,
            });
        }
        let job = Job {
            buf,
            dict: &dict,
            envs: &envs,
            chunks,
            threads: args.threads,
            writers: args.writers,
            sel: &sel,
            order: &order,
            slow_lines: AtomicU64::new(0),
            timing: args.timing,
        };
        let r = match args.format.as_str() {
            "wide" | "csv" => out::write_wide(&job, &per_freq("csv")?),
            "long" => out::write_long(&job, &exact(".long.csv")),
            "bin" | "esobin" => {
                if stdout {
                    return Err("'bin' cannot be written to stdout".into());
                }
                match exact(".esobin") {
                    Target::File(p) => out::write_bin(&job, &p),
                    Target::Stdout => unreachable!(),
                }
            }
            "sqlite" | "sql" | "db" => match exact(".sqlite") {
                Target::Stdout => return Err("'sqlite' cannot be written to stdout".into()),
                Target::File(p) => sqlite::write_sqlite(&job, &p),
            },
            "parquet" | "pq" => {
                if stdout {
                    return Err("'parquet' cannot be written to stdout".into());
                }
                out::write_parquet(&job, &per_freq("parquet")?)
            }
            other => return Err(format!("unknown format '{}' (wide, long, parquet, sqlite, bin)", other)),
        };
        match r {
            Ok(()) => {
                slow = job.slow_lines.load(Ordering::Relaxed);
                break;
            }
            Err(WalkErr::Msg(m)) => return Err(m),
            Err(WalkErr::NeedSequential) if !sequential => {
                if stdout {
                    return Err("this file needs the sequential fallback, which cannot restart on stdout; write to a file instead".into());
                }
                sequential = true;
            }
            Err(WalkErr::NeedSequential) => return Err("internal error: sequential fallback failed".into()),
        }
    }
    let t3 = Instant::now();
    if sinks::BAD_VALUE.load(Ordering::Relaxed) {
        eprintln!("esoconv: warning: some values were not valid numbers and were stored as NaN");
    }
    if args.timing {
        let ms = |a: Instant, b: Instant| (b - a).as_secs_f64() * 1e3;
        eprintln!("  open+dictionary  {:8.2} ms", ms(t0, t1));
        eprintln!("  record scan      {:8.2} ms  ({} timestamp records)", ms(t1, t2), stamps.len());
        eprintln!("  convert          {:8.2} ms  (sequential fallback: {}, slow-path lines: {})", ms(t2, t3), sequential, slow);
        eprintln!("  total            {:8.2} ms  ({:.0} MB/s)", ms(t0, t3), len as f64 / 1e6 / (t3 - t0).as_secs_f64());
    }
    Ok(())
}

fn main() {
    if !(is_x86_feature_detected!("avx2") && is_x86_feature_detected!("bmi1")) {
        eprintln!("esoconv: this build requires a CPU with AVX2");
        std::process::exit(2);
    }
    if let Err(e) = run() {
        eprintln!("esoconv: {}", e);
        std::process::exit(1);
    }
}
