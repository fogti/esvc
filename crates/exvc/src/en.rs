use crate::addr::Address;
use core::fmt;
use esvc_core::Engine;
use std::collections::HashMap;
use std::sync::Mutex;

pub struct ExEngine {
    pub rgxcache: Mutex<HashMap<String, Result<regex::Regex, regex::Error>>>,
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum CommandKind {
    Append(Vec<String>),
    Change(Vec<String>),
    //Copy(Address),
    Delete,
    Insert(Vec<String>),
    //Move(Address),
    Substitute { pat: String, repl: String },
}

impl fmt::Display for CommandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (cmd, xs) = match self {
            Self::Append(x) => ("a", x),
            Self::Change(x) => ("c", x),
            Self::Insert(x) => ("i", x),
            Self::Delete => {
                write!(f, "d")?;
                return Ok(());
            }
            Self::Substitute { pat, repl } => {
                writeln!(f, "s\n{}\n{}", pat, repl)?;
                return Ok(());
            }
        };
        writeln!(f, "{}", cmd)?;
        for i in xs {
            writeln!(f, "{}", i)?;
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Command {
    Normal {
        addr: Address,
        kind: CommandKind,
        // pub switch_autoindent: bool,
    },
    /*
        Global {
            addr: Address,
            invert: bool,
            cmds: Vec<CommandKind>,
        },
    */
}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Command::Normal { addr, kind } => {
                write!(f, "{} {}", addr, kind)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub enum InpCommandKind {
    Print,
    Append,
    Change,
    Delete,
    Insert,
    Substitute,
    //Global { invert: bool },
}

pub fn parse_command(s: &str) -> anyhow::Result<(Address, InpCommandKind)> {
    use InpCommandKind as K;
    let (addr, s) = crate::addr::parse_address(s)?;
    Ok((
        addr,
        if let Some(x) = s.chars().next() {
            match x {
                'a' => K::Append,
                'c' => K::Change,
                'd' => K::Delete,
                'i' => K::Insert,
                's' => K::Substitute,
                //'g' => K::Global { invert: s.chars().nth(2) == Some('!') },
                _ => anyhow::bail!("unknown command '{}'", x),
            }
        } else {
            K::Print
        },
    ))
}

pub fn resolve_addr(dat: &[String], addr: &Address) -> anyhow::Result<Vec<(Vec<String>, bool)>> {
    use Address as A;
    if dat.is_empty() {
        return Ok(if matches!(*addr, A::RngF(0) | A::Last) {
            // for initial insert or such
            vec![(vec![], true)]
        } else {
            vec![]
        });
    }
    Ok(match addr {
        A::Rng(rng) => {
            if rng.start >= dat.len() || rng.start >= rng.end {
                vec![(dat.to_vec(), false)]
            } else if rng.end >= dat.len() {
                let (part1, part2) = dat.split_at(rng.start);
                vec![(part1.to_vec(), false), (part2.to_vec(), true)]
            } else {
                let (part1, part2) = dat.split_at(rng.start);
                let (part2, part3) = part2.split_at(rng.end - rng.start);
                vec![
                    (part1.to_vec(), false),
                    (part2.to_vec(), true),
                    (part3.to_vec(), false),
                ]
            }
        }
        A::RngF(rngstart) => {
            use core::cmp::Ordering as Ordi;
            match rngstart.cmp(&dat.len()) {
                Ordi::Less => {
                    let (part1, part2) = dat.split_at(*rngstart);
                    vec![(part1.to_vec(), false), (part2.to_vec(), true)]
                }
                Ordi::Equal => vec![(dat.to_vec(), false), (vec![], true)],
                Ordi::Greater => vec![(dat.to_vec(), false)],
            }
        }
        A::Rgx(rgx) => {
            let re = regex::Regex::new(rgx)?;
            dat.iter()
                .map(|i| (vec![i.to_string()], re.is_match(i)))
                .collect()
        }
        A::Last => {
            vec![
                (dat[..dat.len() - 1].to_vec(), false),
                (vec![dat.last().unwrap().to_string()], true),
            ]
        }
    })
}

fn run_command(
    rgxcache: &Mutex<HashMap<String, Result<regex::Regex, regex::Error>>>,
    kind: &CommandKind,
    mut dat: Vec<String>,
) -> anyhow::Result<Vec<String>> {
    use CommandKind as K;
    Ok(match kind {
        K::Append(a) => {
            dat.extend(a.iter().cloned());
            dat
        }
        K::Insert(a) => {
            let mut tmp = a.clone();
            tmp.extend(dat);
            tmp
        }
        K::Change(c) => c.clone(),
        K::Delete => vec![],
        K::Substitute { pat, repl } => {
            let mut rgxcache = rgxcache.lock().unwrap();
            let rgx = rgxcache
                .entry(pat.clone())
                .or_insert_with(|| regex::Regex::new(pat))
                .as_ref()
                .map_err(|e| e.clone())?;
            dat.into_iter()
                .map(|i| rgx.replace_all(&i, repl).to_string())
                .collect()
        }
    })
}

struct ErrPropagateFlatten<I> {
    it: I,
    acc: std::collections::VecDeque<String>,
}

impl<I> Iterator for ErrPropagateFlatten<I>
where
    I: Iterator<Item = anyhow::Result<Vec<String>>>,
{
    type Item = anyhow::Result<String>;

    fn next(&mut self) -> Option<anyhow::Result<String>> {
        Some(loop {
            if let Some(x) = self.acc.pop_front() {
                break Ok(x);
            }
            match self.it.next()? {
                Err(e) => break Err(e),
                Ok(x) => self.acc.extend(x),
            }
        })
    }
}

impl Engine for ExEngine {
    type Error = anyhow::Error;
    type Arg = Command;
    type Dat = Vec<String>;

    fn run_event_bare(
        &self,
        cmd: u32,
        arg: &Command,
        dat: &Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        assert_eq!(cmd, 0);
        let (sel, cmds) = match arg {
            Command::Normal { addr, kind } => {
                (resolve_addr(&dat[..], addr)?, core::slice::from_ref(kind))
            } /*
              Command::Global { addr, invert, cmds } => {
                  let mut sel = resolve_addr(&dat[..], addr)?;
                  if *invert {
                      for i in &mut sel {
                          i.1 = !i.1;
                      }
                  }
                  (sel, &**cmds)
              }
              */
        };
        ErrPropagateFlatten {
            it: sel.into_iter().map(|(i, dosmth)| {
                if dosmth {
                    cmds.iter()
                        .try_fold(i, |i, cmd| run_command(&self.rgxcache, cmd, i))
                } else {
                    Ok(i)
                }
            }),
            acc: Default::default(),
        }
        .collect()
    }
}
