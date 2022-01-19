use std::collections::BTreeSet;
use std::str::from_utf8;

fn sev(search: &str, replacement: &str) -> esvc_core::Event<Vec<u8>> {
    esvc_core::Event {
        cmd: 0,
        arg: format!(
            "{{\"search\":\"{}\",\"replacement\":\"{}\"}}",
            search, replacement
        )
        .into(),
        deps: Default::default(),
    }
}

fn main() {
    let mut g = esvc_core::Graph::default();
    let mut e = esvc_wasm::WasmEngine::new().expect("unable to initialize engine");
    e.add_commands(Some(
        std::fs::read("../../../wasm-crates/example-sear/pkg/example_sear_bg.wasm")
            .expect("unable to read module"),
    ))
    .expect("unable to insert module");

    let mut w = esvc_core::WorkCache::new(&e, "Hi, what's up??".to_string().into());

    println!(":: shelve events ::");

    let mut xs = BTreeSet::new();

    for i in [
        sev("Hi", "Hello UwU"),
        sev("UwU", "World"),
        sev("what", "wow"),
        sev("s up", "sup"),
        sev("??", "!"),
        sev("sup!", "soap?"),
        sev("p", "np"),
    ] {
        if let Some(h) = w
            .shelve_event(&mut g, xs.clone(), i)
            .expect("unable to shelve event")
        {
            xs.insert(h);
        }
    }

    println!(
        "expect result: {}",
        "Hi, what's up??"
            .replace("Hi", "Hello UwU")
            .replace("UwU", "World")
            .replace("what", "wow")
            .replace("s up", "sup")
            .replace("??", "!")
            .replace("sup!", "soap?")
            .replace("p", "np")
    );

    println!(":: x ::");
    for i in &xs {
        println!("{}", i);
    }
    println!();

    println!(":: e.graph.events[] ::");
    for (h, ev) in &g.events {
        println!("{} {}", h, from_utf8(&ev.arg[..]).unwrap());
        for (i, &is_hard) in &ev.deps {
            println!(">> {} ({})", i, if is_hard { "hard" } else { "soft" });
        }
        println!();
    }

    println!(":: e.graph as .dot ::");
    println!("{:?}", esvc_core::Dot(&g));

    println!(":: minx ::");
    let minx: BTreeSet<_> = g
        .fold_state(xs.iter().map(|&y| (y, false)).collect(), false)
        .unwrap()
        .into_iter()
        .map(|x| x.0)
        .collect();
    for i in &minx {
        println!("{}", i);
    }
    println!();

    println!(":: applied ::");

    let (res, tt) = w
        .run_foreach_recursively(
            &g,
            minx.iter()
                .map(|&i| (i, esvc_core::IncludeSpec::IncludeAll))
                .collect(),
        )
        .unwrap();
    assert_eq!(xs, tt);
    println!("{}", from_utf8(res).unwrap());
}
