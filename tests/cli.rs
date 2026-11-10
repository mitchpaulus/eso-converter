//! End-to-end tests: run the binary on a small real EnergyPlus file and
//! compare against a deliberately naive reference parser.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const FREQS: [&str; 7] = ["Detailed", "TimeStep", "Hourly", "Daily", "Monthly", "RunPeriod", "Annual"];
const KIND: [usize; 7] = [2, 2, 2, 3, 4, 5, 6];

fn fixture() -> Vec<u8> {
    std::fs::read(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/mixed.eso")).unwrap()
}

fn freq_of(tag: &str) -> usize {
    let t = tag.split('[').next().unwrap().trim().to_lowercase().replace(' ', "");
    match t.as_str() {
        "eachcall" => 0,
        "timestep" => 1,
        "hourly" => 2,
        "daily" => 3,
        "monthly" => 4,
        "runperiod" => 5,
        "annual" => 6,
        other => panic!("frequency {other}"),
    }
}

fn datetime(f: usize, s: &[&str]) -> String {
    let n = |i: usize| s[i].trim().parse::<u32>().unwrap();
    match f {
        0..=2 => {
            let end: f64 = s[6].trim().parse().unwrap();
            let (h, m, sec) = if f == 2 || end >= 60.0 {
                (n(4), 0, 0)
            } else {
                let c = (end * 100.0).round() as u32;
                (n(4) - 1, c / 100, c % 100 * 60 / 100)
            };
            format!("{:02}/{:02} {:02}:{:02}:{:02}", n(1), n(2), h, m, sec)
        }
        3 => format!("{:02}/{:02}", n(1), n(2)),
        4 => format!("{:02}", n(1)),
        5 => String::new(),
        _ => n(0).to_string(),
    }
}

struct Reference {
    /// Wide CSV text per frequency (lower-case name).
    wide: HashMap<String, String>,
    long: String,
}

/// Straightforward line-by-line conversion; `env` selects a 1-based environment.
fn reference(eso: &[u8], env: Option<usize>) -> Reference {
    let mut text = String::from_utf8_lossy(eso).replace("\r\n", "\n");
    // A final line without a newline is incomplete (truncated file) and is ignored.
    text.truncate(text.rfind('\n').map_or(0, |i| i + 1));
    let mut lines = text.split('\n');
    lines.next();
    let mut vars: HashMap<u32, (usize, String, String, String)> = HashMap::new();
    let mut dict_order: Vec<Vec<u32>> = vec![vec![]; 7];
    for l in lines.by_ref() {
        if l == "End of Data Dictionary" {
            break;
        }
        let mut p = l.splitn(3, ',');
        let id: u32 = p.next().unwrap().parse().unwrap();
        p.next();
        if id <= 6 {
            continue;
        }
        let rest = p.next().unwrap();
        let (body, tag) = rest.rsplit_once('!').unwrap();
        let f = freq_of(tag);
        let body = body.trim();
        let (nk, units) = body.rsplit_once('[').unwrap();
        let units = units.trim_end_matches(']').to_string();
        let (key, name) = match nk.split_once(',') {
            Some((k, n)) => (k.trim().to_string(), n.trim().to_string()),
            None => (String::new(), nk.trim().to_string()),
        };
        vars.insert(id, (f, key, name, units));
        dict_order[f].push(id);
    }
    type Row = (String, String, HashMap<u32, String>);
    let mut rows: Vec<Vec<Row>> = vec![vec![]; 7];
    let mut cur: Vec<Option<Row>> = vec![None; 7];
    let mut first: Vec<Option<Vec<u32>>> = vec![None; 7];
    let mut first_done = [false; 7];
    let mut stamps: HashMap<usize, Vec<String>> = HashMap::new();
    let (mut env_title, mut env_idx) = (String::new(), 0usize);
    let mut long = String::from("Environment,Date/Time,Frequency,Key,Variable,Units,Value\n");
    for l in lines {
        if l == "End of Data" {
            break;
        }
        let Some((a, rest)) = l.split_once(',') else { continue };
        let a: u32 = a.parse().unwrap();
        if a <= 6 {
            for f in 0..7 {
                if a == 1 || KIND[f] == a as usize {
                    if let Some(r) = cur[f].take() {
                        rows[f].push(r);
                    }
                    first_done[f] |= first[f].is_some();
                }
            }
            if a == 1 {
                env_title = rest.split(',').next().unwrap().trim().to_string();
                env_idx += 1;
            } else {
                stamps.insert(a as usize, rest.split(',').map(|s| s.to_string()).collect());
            }
            continue;
        }
        let (f, key, name, units) = &vars[&a];
        let f = *f;
        if !first_done[f] {
            let v = first[f].get_or_insert_with(Vec::new);
            if !v.contains(&a) {
                v.push(a);
            }
        }
        if env.is_some_and(|e| e != env_idx) {
            continue;
        }
        let val = rest.split(',').next().unwrap().trim().to_string();
        let st: Vec<&str> = stamps[&KIND[f]].iter().map(|s| s.as_str()).collect();
        let dt = datetime(f, &st);
        long.push_str(&format!("{},{},{},{},{},{},{}\n", env_title, dt, FREQS[f], key, name, units, val));
        cur[f].get_or_insert_with(|| (env_title.clone(), dt, HashMap::new())).2.insert(a, val);
    }
    let mut wide = HashMap::new();
    for f in 0..7 {
        if let Some(r) = cur[f].take() {
            rows[f].push(r);
        }
        if dict_order[f].is_empty() {
            continue;
        }
        // Columns: order of the frequency's first row, then the rest in dictionary order.
        let mut order = first[f].clone().unwrap_or_default();
        for id in &dict_order[f] {
            if !order.contains(id) {
                order.push(*id);
            }
        }
        let mut s = String::from("Environment,Date/Time");
        for id in &order {
            let (_, key, name, units) = &vars[id];
            if key.is_empty() {
                s.push_str(&format!(",{} [{}]({})", name, units, FREQS[f]));
            } else {
                s.push_str(&format!(",{}:{} [{}]({})", key, name, units, FREQS[f]));
            }
        }
        s.push('\n');
        for (e, dt, vals) in &rows[f] {
            s.push_str(&format!("{},{}", e, dt));
            for id in &order {
                s.push(',');
                s.push_str(vals.get(id).map(|v| v.as_str()).unwrap_or(""));
            }
            s.push('\n');
        }
        wide.insert(FREQS[f].to_lowercase(), s);
    }
    Reference { wide, long }
}

struct Scratch(PathBuf);
impl Scratch {
    fn new(name: &str) -> Scratch {
        let d = std::env::temp_dir().join(format!("esoconv-test-{}-{}", std::process::id(), name));
        std::fs::create_dir_all(&d).unwrap();
        Scratch(d)
    }
    fn path(&self, f: &str) -> PathBuf {
        self.0.join(f)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn esoconv(args: &[&str]) -> std::process::Output {
    let out = Command::new(env!("CARGO_BIN_EXE_esoconv")).args(args).output().unwrap();
    assert!(out.status.success(), "esoconv {:?} failed: {}", args, String::from_utf8_lossy(&out.stderr));
    out
}

/// Runs wide + long with `extra` args on `eso` and compares with the reference.
fn check(name: &str, eso: &[u8], env: Option<usize>, extra: &[&str]) {
    let s = Scratch::new(name);
    let input = s.path("in.eso");
    std::fs::write(&input, eso).unwrap();
    let want = reference(eso, env);
    let base = s.path("out");
    let mut args = vec![input.to_str().unwrap(), "-o", base.to_str().unwrap()];
    args.extend_from_slice(extra);
    esoconv(&args);
    for (f, text) in &want.wide {
        let got = std::fs::read_to_string(s.path(&format!("out.{}.csv", f))).unwrap();
        assert!(got == *text, "{name}: wide {f} differs");
    }
    let long = s.path("long.csv");
    let mut args = vec![input.to_str().unwrap(), "-f", "long", "-o", long.to_str().unwrap()];
    args.extend_from_slice(extra);
    esoconv(&args);
    assert!(std::fs::read_to_string(&long).unwrap() == want.long, "{name}: long differs");
}

#[test]
fn wide_and_long_match_reference() {
    check("default", &fixture(), None, &[]);
}

#[test]
fn tiny_chunks_and_odd_thread_counts() {
    check("chunks", &fixture(), None, &["--chunk", "4096", "-j", "3"]);
    check("single", &fixture(), None, &["-j", "1"]);
}

#[test]
fn environment_selection() {
    check("env2", &fixture(), Some(2), &["-e", "2"]);
    check("envname", &fixture(), Some(3), &["-e", "run period"]);
}

#[test]
fn lf_line_endings() {
    let lf = String::from_utf8(fixture()).unwrap().replace("\r\n", "\n").into_bytes();
    check("lf", &lf, None, &["--chunk", "8192"]);
}

#[test]
fn truncated_file() {
    let eso = fixture();
    check("trunc", &eso[..eso.len() * 2 / 3], None, &[]);
}

/// Rows with absent, reordered and repeated values still land in the right cells.
#[test]
fn missing_reordered_and_duplicate_values() {
    let text = String::from_utf8(fixture()).unwrap();
    let mut out: Vec<String> = Vec::new();
    let mut in_data = false;
    let mut n = 0u32;
    for l in text.split("\r\n") {
        let is_value = in_data && l.split(',').next().and_then(|a| a.parse::<u32>().ok()).is_some_and(|a| a > 6);
        if l == "End of Data Dictionary" {
            in_data = true;
        }
        if !is_value {
            out.push(l.to_string());
            continue;
        }
        n = n.wrapping_mul(1664525).wrapping_add(1013904223);
        match (n >> 16) % 40 {
            0 | 1 => {} // drop the value
            2 => {
                // swap with the previous line if that is a value too
                let prev = out.pop().unwrap();
                let prev_is_value = prev.split(',').next().and_then(|a| a.parse::<u32>().ok()).is_some_and(|a| a > 6);
                out.push(l.to_string());
                if prev_is_value {
                    out.push(prev);
                } else {
                    out.insert(out.len() - 1, prev);
                }
            }
            3 => {
                out.push(l.to_string());
                out.push(format!("{},42.5", l.split(',').next().unwrap()));
            }
            _ => out.push(l.to_string()),
        }
    }
    let eso = out.join("\r\n").into_bytes();
    check("messy", &eso, None, &[]);
    check("messy-chunks", &eso, None, &["--chunk", "4096", "-j", "5"]);
}

#[test]
fn stdout_and_filters() {
    let s = Scratch::new("stdout");
    let input = s.path("in.eso");
    std::fs::write(&input, fixture()).unwrap();
    let want = reference(&fixture(), None);
    let out = esoconv(&[input.to_str().unwrap(), "--freq", "hourly", "-o", "-"]);
    assert!(String::from_utf8(out.stdout).unwrap() == want.wide["hourly"]);
    // --var keeps only matching columns, in the same order
    let out = esoconv(&[input.to_str().unwrap(), "--freq", "timestep", "-v", "drybulb", "-o", "-"]);
    let got = String::from_utf8(out.stdout).unwrap();
    let header = got.lines().next().unwrap();
    assert_eq!(header, "Environment,Date/Time,Environment:Site Outdoor Air Drybulb Temperature [C](TimeStep)");
    let full = &want.wide["timestep"];
    let col = full.lines().next().unwrap().split(',').position(|c| c.contains("Drybulb")).unwrap();
    for (a, b) in got.lines().zip(full.lines()).skip(1) {
        assert_eq!(a.split(',').nth(2), b.split(',').nth(col));
    }
}

#[test]
fn binary_formats_are_well_formed() {
    let s = Scratch::new("bin");
    let input = s.path("in.eso");
    std::fs::write(&input, fixture()).unwrap();
    let want = reference(&fixture(), None);
    let i = input.to_str().unwrap();

    esoconv(&[i, "-f", "parquet", "-o", s.path("p").to_str().unwrap()]);
    let p = std::fs::read(s.path("p.hourly.parquet")).unwrap();
    assert!(p.starts_with(b"PAR1") && p.ends_with(b"PAR1"));

    esoconv(&[i, "-f", "sqlite", "-o", s.path("d.sqlite").to_str().unwrap()]);
    let d = std::fs::read(s.path("d.sqlite")).unwrap();
    assert!(d.starts_with(b"SQLite format 3\0") && d.len().is_multiple_of(4096));
    assert_eq!(u32::from_be_bytes(d[28..32].try_into().unwrap()) as usize, d.len() / 4096);

    // esobin: the hourly table must hold exactly the numbers of the wide CSV.
    esoconv(&[i, "-f", "bin", "-o", s.path("b.esobin").to_str().unwrap()]);
    let b = std::fs::read(s.path("b.esobin")).unwrap();
    assert!(b.starts_with(b"ESOBIN01"));
    let n = u64::from_le_bytes(b[8..16].try_into().unwrap()) as usize;
    let json = std::str::from_utf8(&b[16..16 + n]).unwrap();
    let base = (16 + n + 63) & !63;
    let t = &json[json.find("\"frequency\":\"Hourly\"").unwrap()..];
    let num = |key: &str| -> usize {
        let a = t.find(key).unwrap() + key.len();
        t[a..].split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
    };
    let (rows, off) = (num("\"rows\":"), num("\"data_offset\":"));
    let csv: Vec<Vec<&str>> = want.wide["hourly"].lines().skip(1).map(|l| l.split(',').collect()).collect();
    assert_eq!(rows, csv.len());
    let cols = csv[0].len() - 2;
    for c in 0..cols {
        for r in 0..rows {
            let o = base + off + (c * rows + r) * 8;
            let x = f64::from_le_bytes(b[o..o + 8].try_into().unwrap());
            assert_eq!(x, csv[r][c + 2].parse::<f64>().unwrap(), "row {r} col {c}");
        }
    }
}

#[test]
fn rejects_non_eso_input() {
    let s = Scratch::new("bad");
    let input = s.path("in.eso");
    std::fs::write(&input, b"hello\nworld\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_esoconv")).arg(&input).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("is this an .eso file"));
}
