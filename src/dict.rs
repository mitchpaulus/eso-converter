//! Data dictionary (the header section of an .eso file).

pub const NFREQ: usize = 7;
pub const FREQ_NAMES: [&str; NFREQ] = ["Detailed", "TimeStep", "Hourly", "Daily", "Monthly", "RunPeriod", "Annual"];
/// Which timestamp record (2..=6) carries the time for each frequency.
pub const FREQ_KIND: [u8; NFREQ] = [2, 2, 2, 3, 4, 5, 6];

pub fn parse_freq(s: &str) -> Option<usize> {
    let t = s.trim().to_ascii_lowercase();
    let t = t.replace([' ', '_', '-'], "");
    Some(match t.as_str() {
        "detailed" | "eachcall" => 0,
        "timestep" => 1,
        "hourly" => 2,
        "daily" => 3,
        "monthly" => 4,
        "runperiod" => 5,
        "annual" => 6,
        _ => return None,
    })
}

pub struct Var {
    pub id: u32,
    pub freq: u8,
    pub key: String,
    pub name: String,
    pub units: String,
}

impl Var {
    /// `KEY:Name [units](Frequency)`, the ReadVarsESO column label.
    pub fn label(&self) -> String {
        let f = FREQ_NAMES[self.freq as usize];
        if self.key.is_empty() {
            format!("{} [{}]({})", self.name, self.units, f)
        } else {
            format!("{}:{} [{}]({})", self.key, self.name, self.units, f)
        }
    }
}

pub struct Dict {
    pub version: String,
    pub vars: Vec<Var>,
    pub nseq: [usize; NFREQ],
    /// Offset of the first data record.
    pub data_start: usize,
    pub crlf: bool,
    /// id -> variable index + 1 (0 = unknown).
    pub idmap: Vec<u32>,
}

fn lossy(b: &[u8]) -> String {
    String::from_utf8_lossy(b).trim().to_string()
}

pub fn parse(buf: &[u8]) -> Result<Dict, String> {
    let mut d = Dict {
        version: String::new(),
        vars: Vec::new(),
        nseq: [0; NFREQ],
        data_start: 0,
        crlf: false,
        idmap: Vec::new(),
    };
    let mut p = 0usize;
    let mut lineno = 0usize;
    loop {
        if p >= buf.len() {
            return Err("no 'End of Data Dictionary' line found; is this an .eso file?".into());
        }
        let e = buf[p..].iter().position(|&c| c == b'\n').map(|i| p + i).unwrap_or(buf.len());
        let mut line = &buf[p..e];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
            if lineno == 0 {
                d.crlf = true;
            }
        }
        let next = (e + 1).min(buf.len());
        if lineno == 0 {
            if !line.starts_with(b"Program Version") {
                return Err("file does not start with 'Program Version'; is this an .eso file?".into());
            }
            d.version = lossy(line);
        } else if line.starts_with(b"End of Data Dictionary") {
            d.data_start = next;
            let max_id = d.vars.iter().map(|v| v.id).max().unwrap_or(0) as usize;
            if max_id > 200_000_000 {
                return Err("variable ids are implausibly large".into());
            }
            d.idmap = vec![0u32; max_id + 1];
            for (i, v) in d.vars.iter().enumerate() {
                if d.idmap[v.id as usize] != 0 {
                    return Err(format!("duplicate variable id {} in dictionary", v.id));
                }
                d.idmap[v.id as usize] = i as u32 + 1;
            }
            return Ok(d);
        } else if !line.is_empty() {
            parse_line(line, &mut d).map_err(|m| format!("dictionary line {}: {}", lineno + 1, m))?;
        }
        p = next;
        lineno += 1;
    }
}

fn parse_line(line: &[u8], d: &mut Dict) -> Result<(), String> {
    let c1 = line.iter().position(|&c| c == b',').ok_or("missing id")?;
    let id: u32 = std::str::from_utf8(&line[..c1]).ok().and_then(|s| s.trim().parse().ok()).ok_or("bad id")?;
    if id <= 6 {
        return Ok(());
    }
    let rest = &line[c1 + 1..];
    let c2 = rest.iter().position(|&c| c == b',').ok_or("missing value count")?;
    let rest = &rest[c2 + 1..];
    let bang = rest.iter().rposition(|&c| c == b'!').ok_or("missing '!Frequency'")?;
    let ftxt = String::from_utf8_lossy(&rest[bang + 1..]).to_string();
    let fword = ftxt.split('[').next().unwrap_or("");
    let freq = parse_freq(fword).ok_or_else(|| format!("unknown reporting frequency '{}'", fword.trim()))?;
    let body = &rest[..bang];
    let (nk, units) = match (body.iter().rposition(|&c| c == b'['), body.iter().rposition(|&c| c == b']')) {
        (Some(a), Some(b)) if a < b => (&body[..a], lossy(&body[a + 1..b])),
        _ => (body, String::new()),
    };
    let (key, name) = match nk.iter().position(|&c| c == b',') {
        Some(c) => (lossy(&nk[..c]), lossy(&nk[c + 1..])),
        None => (String::new(), lossy(nk)),
    };
    d.nseq[freq] += 1;
    d.vars.push(Var {
        id,
        freq: freq as u8,
        key,
        name,
        units,
    });
    Ok(())
}
