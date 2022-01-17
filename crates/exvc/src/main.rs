use ansi_term::Colour;
use esvc_core::{Graph, WorkCache};
use std::io::Write;

mod addr;
mod en;

type Arg = <en::ExEngine as esvc_core::Engine>::Arg;

// TODO: add support for merging/rebasing

struct Context<'en> {
    path: Option<String>,
    g: Graph<Arg>,
    w: WorkCache<'en, en::ExEngine>,
}

fn rewrap_wce(e: esvc_core::WorkCacheError<anyhow::Error>) -> anyhow::Error {
    use esvc_core::WorkCacheError as Wce;
    match e {
        Wce::CommandNotFound(e) => Wce::<core::convert::Infallible>::CommandNotFound(e).into(),
        Wce::Graph(e) => Wce::<core::convert::Infallible>::Graph(e).into(),
        Wce::Engine(e) => e,
    }
}

impl Context<'_> {
    fn fullic(&mut self, line: &str) -> anyhow::Result<bool> {
        Ok(if line == "*dot" {
            print!("{}", esvc_core::Dot(&self.g));
            true
        } else if line == "*state" {
            esvc_core::print_deps(
                &mut std::io::stdout(),
                &format!("{} ", Colour::Blue.paint(">>"),),
                self.g.nstates[""].iter().copied(),
            )?;
            true
        } else if line == "w" {
            if let Some(path) = &self.path {
                let f = std::fs::File::create(path)?;
                let mut fz = zstd::stream::write::Encoder::new(f, 20)?;
                bincode::serialize_into(&mut fz, &self.g)?;
                fz.finish()?.sync_all()?;
                true
            } else {
                anyhow::bail!("no file path is associated with this session");
            }
        } else {
            false
        })
    }

    fn rick(&mut self, addr: addr::Address, ick: en::InpCommandKind) -> anyhow::Result<()> {
        use en::InpCommandKind as Ick;
        let state = &self.g.nstates[""];
        let pipelcmd = match ick {
            Ick::Print => {
                let (res, _) = self
                    .w
                    .run_foreach_recursively(
                        &self.g,
                        state
                            .iter()
                            .map(|&i| (i, esvc_core::IncludeSpec::IncludeAll))
                            .collect(),
                    )
                    .map_err(rewrap_wce)?;
                let mut lnum = 0;
                for (lines, dosmth) in en::resolve_addr(res, &addr)?.into_iter() {
                    if dosmth {
                        for line in lines {
                            println!(
                                "{}: {}",
                                Colour::Fixed(240).paint(format!("{:>5}", lnum)),
                                line
                            );
                            lnum += 1;
                        }
                    } else {
                        lnum += lines.len();
                    }
                }
                return Ok(());
            }
            Ick::Delete => en::Command::Normal {
                addr,
                kind: en::CommandKind::Delete,
            },
            _ => {
                let mut line = String::new();
                let stdin = std::io::stdin();
                let mut ls = Vec::new();

                loop {
                    stdin.read_line(&mut line)?;
                    let line_ = line.trim_end_matches(&['\r', '\n'][..]);
                    if line_ == "." {
                        break;
                    }
                    ls.push(line_.to_string());
                    line.clear();
                }

                let kind = match ick {
                    Ick::Append => en::CommandKind::Append(ls),
                    Ick::Change => en::CommandKind::Change(ls),
                    Ick::Insert => en::CommandKind::Insert(ls),
                    Ick::Substitute => {
                        if let [pat, repl] = &ls[..] {
                            en::CommandKind::Substitute {
                                pat: pat.to_string(),
                                repl: repl.to_string(),
                            }
                        } else {
                            anyhow::bail!("substitute: invalid input line count (!= 2)");
                        }
                    }
                    _ => anyhow::bail!("(internal) unknown command: {:?}", ick),
                };
                en::Command::Normal { addr, kind }
            }
        };

        let state = self.g.nstates[""].clone();
        if let Some(h) = self
            .w
            .shelve_event(
                &mut self.g,
                state,
                esvc_core::Event {
                    cmd: 0,
                    arg: pipelcmd,
                    deps: Default::default(),
                },
            )
            .map_err(rewrap_wce)?
        {
            println!("{} {}", Colour::Blue.paint(">>"), h);
            if self.g.nstates[""].len() > 100 {
                let st = match self.g.fold_state(
                    self.g.nstates[""]
                        .iter()
                        .chain(core::iter::once(&h))
                        .map(|&y| (y, false))
                        .collect(),
                    false,
                ) {
                    Some(x) => x,
                    None => anyhow::bail!("unable to resolve dependencies of current state"),
                }
                .into_iter()
                .map(|x| x.0)
                .collect();
                self.g.nstates.insert(String::new(), st);
            } else {
                self.g.nstates.get_mut("").unwrap().insert(h);
            }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let arg = std::env::args().nth(1);
    let e = en::ExEngine {
        rgxcache: Default::default(),
    };
    let mut ctx = Context {
        path: None,
        g: if let Some(arg) = &arg {
            if std::path::Path::new(arg).exists() {
                let f = std::io::BufReader::new(std::fs::File::open(arg)?);
                let fz = zstd::stream::read::Decoder::new(f)?;
                bincode::deserialize_from::<_, Graph<Arg>>(fz)?
            } else if arg == "--help" {
                println!("USAGE: exvc [GRAPH_FILE]");
                return Ok(());
            } else {
                Graph::default()
            }
        } else {
            Graph::default()
        },
        w: WorkCache::new(&e, vec![]),
    };
    ctx.path = arg;

    let is_atty = atty::is(atty::Stream::Stdin) && atty::is(atty::Stream::Stdout);
    let mut stdout = std::io::stdout();
    let stdin = std::io::stdin();
    let mut line = String::new();

    if !ctx.g.nstates.contains_key("") {
        ctx.g.nstates.insert(String::new(), Default::default());
    }

    loop {
        if is_atty {
            write!(&mut stdout, ":")?;
            stdout.flush()?;
        }
        line.clear();
        stdin.read_line(&mut line)?;
        line.truncate(line.trim_end_matches(&['\r', '\n'][..]).len());
        if ctx.fullic(&line)? {
            continue;
        } else if line == "q!" {
            break;
        }

        let (addr, ick) = match en::parse_command(&line) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("{} {}", Colour::Red.paint("E:"), e);
                continue;
            }
        };

        if let Err(e) = ctx.rick(addr, ick) {
            eprintln!("{} {}", Colour::Red.paint("E:"), e);
        }
    }

    Ok(())
}
