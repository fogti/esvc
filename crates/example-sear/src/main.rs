use std::collections::BTreeSet;
use std::str::from_utf8;

fn sev(search: &str, replacement: &str) -> esvc_core::Event {
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
    let mut e = esvc_core::Engine::new().expect("unable to initialize engine");
    e.add_command(
        std::fs::read("../../../wasm-crates/example-sear/pkg/example_sear_bg.wasm")
            .expect("unable to read module"),
    )
    .expect("unable to insert module");

    let mut w = esvc_core::WorkCache::new("Hi, what's up??".to_string().into());

    println!(":: shelve events ::");

    let x = w
        .shelve_events(
            &mut e,
            Default::default(),
            vec![
                sev("Hi", "Hello UwU"),
                sev("UwU", "World"),
                sev("what", "wow"),
                sev("s up", "sup"),
                sev("??", "!"),
                sev("sup!", "soap?"),
                sev("p", "np"),
            ],
        )
        .expect("unable to shelve events");

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
    for &i in &x {
        println!("{}", i);
    }
    println!();

    println!(":: e.graph.events[] ::");
    for (h, ev) in &e.graph().events {
        println!("{} {}", h, from_utf8(&ev.arg[..]).unwrap());
        for i in &ev.deps {
            println!(">> {}", i);
        }
        println!();
    }

    println!(":: minx ::");
    let minx: BTreeSet<_> = e
        .graph()
        .fold_state(x.iter().map(|&y| (y, false)).collect(), false)
        .unwrap()
        .into_iter()
        .map(|x| x.0)
        .collect();
    for i in &minx {
        println!("{}", i);
    }
    println!();

    let x: BTreeSet<_> = x.into_iter().collect();

    println!(":: applied ::");

    let mut tt = Default::default();
    for i in &minx {
        let (res, tt2) = w.run_recursively(&e, tt, *i, true).unwrap();
        tt = tt2;
        println!(">> {}", from_utf8(res).unwrap());
    }
    assert_eq!(x, tt);
    println!("{}", from_utf8(&w.0[&x]).unwrap());
}
