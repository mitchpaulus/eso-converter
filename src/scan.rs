//! SIMD search for record-start patterns.
use std::arch::x86_64::*;

/// Calls `f(line_start)` for every line whose first byte is in `lo..=hi` and
/// whose second byte is `,`. Only newlines at positions in `from..to` are
/// considered (the reported offset is newline + 1). `f` returns false to stop.
pub fn scan_lines(buf: &[u8], from: usize, to: usize, lo: u8, hi: u8, mut f: impl FnMut(usize) -> bool) {
    let to = to.min(buf.len());
    let mut i = from;
    unsafe {
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let cm = _mm256_set1_epi8(b',' as i8);
        let lo1 = _mm256_set1_epi8((lo - 1) as i8);
        let hi1 = _mm256_set1_epi8((hi + 1) as i8);
        let p = buf.as_ptr();
        while i < to && i + 34 <= buf.len() {
            let a = _mm256_loadu_si256(p.add(i) as *const __m256i);
            let b = _mm256_loadu_si256(p.add(i + 1) as *const __m256i);
            let c = _mm256_loadu_si256(p.add(i + 2) as *const __m256i);
            let m = _mm256_and_si256(
                _mm256_and_si256(_mm256_cmpeq_epi8(a, nl), _mm256_cmpeq_epi8(c, cm)),
                _mm256_and_si256(_mm256_cmpgt_epi8(b, lo1), _mm256_cmpgt_epi8(hi1, b)),
            );
            let mut bits = _mm256_movemask_epi8(m) as u32;
            while bits != 0 {
                let pos = i + bits.trailing_zeros() as usize;
                if pos >= to || !f(pos + 1) {
                    return;
                }
                bits &= bits - 1;
            }
            i += 32;
        }
    }
    while i < to && i + 2 < buf.len() {
        if buf[i] == b'\n' && buf[i + 1] >= lo && buf[i + 1] <= hi && buf[i + 2] == b',' && !f(i + 1) {
            return;
        }
        i += 1;
    }
}

pub fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    let mut i = 0;
    unsafe {
        let k = _mm256_set1_epi8(c as i8);
        while i + 32 <= s.len() {
            let a = _mm256_loadu_si256(s.as_ptr().add(i) as *const __m256i);
            let m = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, k)) as u32;
            if m != 0 {
                return Some(i + m.trailing_zeros() as usize);
            }
            i += 32;
        }
    }
    s[i..].iter().position(|&b| b == c).map(|j| i + j)
}
