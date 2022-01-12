use esvc_indra::id_to_base32;
use std::io::{BufRead, Write};
use std::sync::Arc;

macro_rules! cmdlhdln1 {
    ($items:ident, $exp:expr) => {{
        match $items.next() {
            Some(Ok(x)) => x,
            _ => {
                eprintln!("ERROR: unexpected EOF or error, expected {}", $exp);
                continue;
            }
        }
    }};
}
macro_rules! cmdlhdln2 {
    ($items:ident, $exp:expr) => {{
        match $items.next() {
            Some(x) => x,
            _ => {
                eprintln!("ERROR: unexpected EOF or error, expected {}", $exp);
                continue;
            }
        }
    }};
}

#[derive(Clone, Default, PartialEq)]
struct MyState {
    cmdmap: Arc<Vec<Vec<String>>>,
    data: Vec<u8>,
}

impl esvc_core::state::State for MyState {
    type Error = std::io::Error;

    fn run(&mut self, ev: &esvc_core::Event) -> std::io::Result<()> {
        use std::io::{Error, ErrorKind};
        if let Some(x) = self
            .cmdmap
            .get(usize::try_from(ev.name).expect("unable to convert command id"))
        {
            if x.is_empty() {
                println!("cmd[{}] ignored", id_to_base32(ev.name));
                return Ok(());
            }
            use std::process::{Command, Stdio};
            let orig_data_len = self.data.len();
            let mut chld = Command::new(&x[0])
                .args(&x[1..])
                .arg(std::str::from_utf8(&ev.arg[..]).expect("utf8 argument"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()?;
            let mut stdin = chld.stdin.take().unwrap();
            stdin.write_all(&self.data[..])?;
            stdin.flush()?;
            let _ = stdin; // unblock child process
            let outp = chld.wait_with_output()?;
            if outp.status.success() {
                self.data = outp.stdout;
                eprintln!(
                    "debug[{}].dlen : {} -> {}",
                    id_to_base32(ev.name),
                    orig_data_len,
                    self.data.len()
                );
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::Other,
                    format!("cmd[{}] $? = {}", id_to_base32(ev.name), outp.status),
                ))
            }
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                "event with non-associated name",
            ))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut args = std::env::args().skip(1);

    let config = args.next().unwrap_or_else(|| "--help".to_string());
    if config == "--help" {
        println!("USAGE: esvc-indra CMDCONFIG [DBPATH]");
        return Ok(());
    }

    let mut state = esvc_core::state::HiState {
        top: std::collections::BTreeSet::new(),
        inner: MyState {
            cmdmap: Arc::new(
                std::io::BufReader::new(std::fs::File::open(config)?)
                    .lines()
                    .map(|l| {
                        l.map(|l2| {
                            yz_string_utils::ShellwordSplitter::new(&l2)
                                .map(|i| i.expect("invalid command in cmdlist").into_owned())
                                .collect()
                        })
                    })
                    .collect::<Result<_, _>>()?,
            ),
            data: Vec::new(),
        },
    };

    println!("registered commands: {:?}", state.inner.cmdmap);

    // $ for variable deref
    // % for base32 decode

    let db = match args.next() {
        None => {
            eprintln!("NOTE: persistence disabled");
            indradb::MemoryDatastore::default()
        }
        Some(path) if std::path::Path::new(&*path).exists() => {
            indradb::MemoryDatastore::read(path)?
        }
        Some(path) => indradb::MemoryDatastore::create(path)?,
    };

    let stdin = std::io::stdin();
    let mut vars = std::collections::HashMap::<String, u128>::new();
    let mut line = String::new();
    loop {
        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        print!("esvc $ ");
        std::io::stdout().flush()?;
        let mut items = yz_string_utils::ShellwordSplitter::new(&*line);

        if let Some(Ok(mut x)) = items.next() {
            let mut asgn_varn = None;
            if x == "set" {
                asgn_varn = Some(cmdlhdln1!(items, "variable name"));
                x = cmdlhdln1!(items, "repl command");
            }
            enum CmdArg {
                Lit(String),
                Id(u128),
            }
            let mut items = match items
                .map(|i| {
                    i.map(|i| {
                        if let Some(j) = i.strip_prefix('$') {
                            if let Some(y) = vars.get(j) {
                                Ok(CmdArg::Id(*y))
                            } else {
                                Err(i)
                            }
                        } else if let Some(j) = i.strip_prefix('%') {
                            if let Some(y) = esvc_indra::base32_to_id(j) {
                                Ok(CmdArg::Id(y))
                            } else {
                                Err(i)
                            }
                        } else if let Some(j) = i.strip_prefix('\\') {
                            Ok(CmdArg::Lit(j.to_string()))
                        } else {
                            Ok(CmdArg::Lit(i.to_string()))
                        }
                    })
                })
                .collect::<Result<Result<Vec<_>, _>, _>>()
            {
                Ok(Ok(x)) => x.into_iter(),
                Ok(Err(e)) => {
                    eprintln!("unable to resolve argument: {}", e);
                    continue;
                }
                Err(e) => {
                    eprintln!("unable to parse arguments: {:?}", e);
                    continue;
                }
            };
            let res = match &*x {
                "init" => {
                    // USAGE: init CMDID EARG [DEPS...]
                    let name = match cmdlhdln2!(items, "command id / event name") {
                        CmdArg::Lit(l) => {
                            eprintln!("invalid command id / event name: {}", l);
                            continue;
                        }
                        CmdArg::Id(y) => y,
                    };
                    let arg = match cmdlhdln2!(items, "command arg / event arg") {
                        CmdArg::Lit(l) => l.to_string().into_bytes(),
                        CmdArg::Id(y) => {
                            eprintln!("invalid command arg: {}", id_to_base32(y));
                            continue;
                        }
                    };
                    let deps = match items
                        .map(|y| match y {
                            CmdArg::Lit(l) => Err(l),
                            CmdArg::Id(did) => Ok(did),
                        })
                        .collect::<Result<_, _>>()
                    {
                        Ok(x) => x,
                        Err(e) => {
                            eprintln!("invalid dependency {}", e);
                            continue;
                        }
                    };

                    match esvc_indra::ensure_node(
                        &db,
                        &esvc_core::EventWithDeps {
                            ev: esvc_core::Event { name, arg },
                            deps,
                        },
                    ) {
                        Ok(x) => Some(x),
                        Err(e) => {
                            eprintln!("database error: {:?}", e);
                            continue;
                        }
                    }
                }
                "run" => {
                    // USAGE: run $initres
                    let eid = match cmdlhdln2!(items, "event id") {
                        CmdArg::Lit(l) => {
                            eprintln!("invalid event id: {}", l);
                            continue;
                        }
                        CmdArg::Id(y) => y,
                    };
                    let evwd = match esvc_indra::get_event(&db, eid) {
                        Ok(x) => x,
                        Err(e) => {
                            eprintln!("database error: {:?}", e);
                            continue;
                        }
                    };
                    if let Err(e) = state.run(eid, &evwd.deps, &evwd.ev) {
                        eprintln!("state/run error: {:?}", e);
                    }
                    // TODO: call `cleanup_top`
                    None
                }
                _ => {
                    eprintln!("ERROR: unknown command: {}", x);
                    continue;
                }
            };
            if let Some(res) = res {
                if let Some(varn) = asgn_varn {
                    vars.insert(varn.to_string(), res);
                }
            }
        } else {
            continue;
        }
    }
    Ok(())
}
