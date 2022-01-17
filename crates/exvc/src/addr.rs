use anyhow::Result;
use core::fmt;
use core::ops::{Range, RangeFrom};

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Address {
    Rgx(String),
    Rng(Range<usize>),
    RngF(usize),
    Last,
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Address::Rgx(rgx) => write!(f, "/{}/", rgx.replace("\\", "\\\\").replace("/", "\\/")),
            Address::Rng(rng) => write!(f, "{},{}", rng.start, rng.end),
            Address::RngF(rngst) => write!(f, "{}, ", rngst),
            Address::Last => write!(f, "$"),
        }
    }
}

impl From<Range<usize>> for Address {
    fn from(r: Range<usize>) -> Self {
        Self::Rng(r)
    }
}
impl From<RangeFrom<usize>> for Address {
    fn from(r: RangeFrom<usize>) -> Self {
        Self::RngF(r.start)
    }
}

fn parse_lnum(s: &str) -> Option<(usize, &str)> {
    let eonumidx = s
        .char_indices()
        .take_while(|(_, i)| i.is_ascii_digit())
        .last()?
        .0
        + 1;
    let (numpart, rest) = s.split_at(eonumidx);
    let num = numpart.parse().unwrap();
    Some((num, rest))
}

pub fn parse_address(s: &str) -> Result<(Address, &str)> {
    if let Some(s) = s.strip_prefix('$') {
        Ok((Address::Last, s))
    } else if let Some(s) = s.strip_prefix('/') {
        let mut escaped = false;
        let mut it = s.chars();
        let pat: String = it
            .by_ref()
            .filter_map(|i| {
                let ret = match i {
                    '\'' if !escaped => {
                        escaped = true;
                        return None;
                    }
                    _ if escaped => Some(match i {
                        '\'' | '/' => i,
                        'n' => '\n',
                        't' => '\t',
                        // TODO: warn about this case
                        _ => i,
                    }),
                    '/' => None,
                    _ => Some(i),
                };
                escaped = false;
                Some(ret)
            })
            .map_while(core::convert::identity)
            .collect();
        if escaped {
            anyhow::bail!("regex: escaped EOL");
        }
        Ok((Address::Rgx(pat), it.as_str()))
    } else if let Some((start, s)) = parse_lnum(s) {
        Ok(if let Some(s) = s.strip_prefix(',') {
            if let Some((end, s)) = parse_lnum(s) {
                if start < end {
                    (Address::Rng(start..end), s)
                } else {
                    anyhow::bail!("addr: unable to parse range {},{}", start, end);
                }
            } else {
                (Address::RngF(start), s)
            }
        } else {
            (Address::Rng(start..start + 1), s)
        })
    } else {
        anyhow::bail!("addr: unable to parse address at '{}'", s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addr() {
        macro_rules! xsimple {
            ($x:expr, $addr:expr, $post:expr) => {{
                assert_eq!(parse_address($x).unwrap(), (Address::from($addr), $post));
            }};
        }
        use Address::{Last, Rgx};
        xsimple!("0", 0..1, "");
        xsimple!("0,", 0.., "");
        xsimple!("1", 1..2, "");
        xsimple!("$", Last, "");
        xsimple!("$1", Last, "1");

        xsimple!("/hewwo?/", Rgx("hewwo?".to_string()), "");
        xsimple!("/hewwo?/i", Rgx("hewwo?".to_string()), "i");
    }
}
