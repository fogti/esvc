use ansi_term::Colour;
use esvc_core::{Graph, WorkCache};
use std::io::Write;
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSet;
use syntect::util::as_24_bit_terminal_escaped;

mod addr;
mod en;

type Arg = <en::ExEngine as esvc_core::Engine>::Arg;

struct Context<'en> {
    path: Option<camino::Utf8PathBuf>,
    ps: SyntaxSet,
    ts: ThemeSet,
    g: Graph<Arg>,
    w: WorkCache<'en, en::ExEngine>,
}

fn rewrap_wce(e: esvc_core::WorkCacheError<anyhow::Error>) -> anyhow::Error {
    use core::convert::Infallible as Inf;
    use esvc_core::WorkCacheError as Wce;
    match e {
        Wce::CommandNotFound(e) => Wce::<Inf>::CommandNotFound(e).into(),
        Wce::Graph(e) => Wce::<Inf>::Graph(e).into(),
        Wce::HashChangeAtMerge(a, b) => Wce::<Inf>::HashChangeAtMerge(a, b).into(),
        Wce::NoopAtMerge(h) => Wce::<Inf>::NoopAtMerge(h).into(),
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
        } else if line == "m<" {
            let mut line = String::new();
            let stdin = std::io::stdin();
            stdin.read_line(&mut line)?;
            line.truncate(line.trim_end_matches(&['\r', '\n'][..]).len());

            let f = std::io::BufReader::new(std::fs::File::open(line)?);
            let fz = zstd::stream::read::Decoder::new(f)?;
            let tmpgraph = bincode::deserialize_from::<_, Graph<Arg>>(fz)?;

            let other_estate = tmpgraph
                .nstates
                .get("")
                .ok_or_else(|| anyhow::anyhow!("other file doesn't contain state set"))?;
            let full_odeps = tmpgraph.calculate_dependencies(
                Default::default(),
                other_estate
                    .iter()
                    .map(|&i| (i, esvc_core::IncludeSpec::IncludeAll))
                    .collect(),
            )?;

            for i in full_odeps {
                print!(".");
                let (coll, h) = self.g.ensure_event(tmpgraph.events[&i].clone());
                if let Some(coll) = coll {
                    anyhow::bail!(
                        "hash collision @ {} detected during insertion of {:?}",
                        h,
                        coll,
                    );
                }
            }
            println!();
            println!("minimize state...");
            let xsts = self.g.nstates[""]
                .iter()
                .chain(other_estate.iter())
                .map(|&h| (h, false))
                .collect();
            let xsts: std::collections::BTreeSet<_> = self
                .g
                .fold_state(xsts, false)?
                .into_iter()
                .map(|(h, _)| h)
                .collect();
            println!("try to merge...");
            self.w
                .try_merge(&mut self.g, xsts.clone())
                .map_err(rewrap_wce)?;
            println!("{}", Colour::Green.paint("OK"));
            for h in &xsts {
                println!("{} {}", Colour::Blue.paint(">>"), h);
            }
            if !xsts.is_empty() {
                self.g.nstates.insert(String::new(), xsts);
            }
            true
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
                let it = en::resolve_addr(res, &addr)?.into_iter();
                if let Some(syntax) = self
                    .path
                    .as_ref()
                    .and_then(|p| p.extension())
                    .and_then(|ext| self.ps.find_syntax_by_extension(ext))
                {
                    let mut h = HighlightLines::new(
                        syntax,
                        &self.ts.themes[core::option_env!("EXVC_DEFAULT_THEME")
                            .unwrap_or("base16-mocha.dark")],
                    );
                    for (lines, dosmth) in it {
                        for line in lines {
                            // the highlighting needs to be kept in sync
                            let ranges: Vec<(Style, &str)> = h.highlight(&line, &self.ps);
                            if dosmth {
                                let escaped = as_24_bit_terminal_escaped(&ranges[..], true);
                                println!(
                                    "{}: {}\x1b[0m",
                                    Colour::Fixed(240).paint(format!("{:>5}", lnum)),
                                    escaped
                                );
                            }
                            lnum += 1;
                        }
                    }
                } else {
                    for (lines, dosmth) in it {
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
                let st = self
                    .g
                    .fold_state(
                        self.g.nstates[""]
                            .iter()
                            .chain(core::iter::once(&h))
                            .map(|&y| (y, false))
                            .collect(),
                        false,
                    )?
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
    #[cfg(feature = "tracing_")]
    tracing_subscriber::fmt::init();

    let arg = std::env::args().nth(1);
    let e = en::ExEngine {
        rgxcache: Default::default(),
    };
    let mut ctx = Context {
        path: None,
        ps: SyntaxSet::load_defaults_newlines(),
        ts: ThemeSet::load_defaults(),
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
    ctx.path = arg.map(Into::into);

    {
        let dfl_thpath: Option<&'static str> = core::option_env!("EXVC_DFL_THEME_PATH");
        if let Some(x) = dfl_thpath {
            let themename: Option<&'static str> = core::option_env!("EXVC_DEFAULT_THEME");
            let mut theme = ThemeSet::get_theme(x)?;
            theme.settings.background = Some(syntect::highlighting::Color::BLACK);
            ctx.ts.themes.insert(themename.unwrap().to_string(), theme);
        }
    }

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
        let tmp = match ctx.fullic(&line) {
            Ok(x) => x,
            Err(e) => {
                eprintln!("{} {}", Colour::Red.paint("E:"), e);
                continue;
            }
        };
        if tmp {
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
