#![no_main]
use libfuzzer_sys::fuzz_target;
use once_cell::sync::Lazy;
use std::collections::BTreeSet;
use std::str::from_utf8;

static E: Lazy<esvc_core::Engine> = Lazy::new(|| {
    let mut e = esvc_core::Engine::new().expect("unable to initialize engine");
    e.add_command(
        include_bytes!("../../../../wasm-crates/example-sear/pkg/example_sear_bg.wasm").to_vec()
    )
    .expect("unable to insert module");
    e
});

struct SearEvent<'a> {
    search: &'a str,
    replacement: &'a str,
}

impl From<SearEvent<'_>> for esvc_core::Event {
    fn from(ev: SearEvent<'_>) -> esvc_core::Event {
        esvc_core::Event {
            cmd: 0,
            arg: serde_json::to_string(&serde_json::json!({
                "search": ev.search,
                "replacement": ev.replacement,
            }))
            .unwrap()
            .into(),
            deps: Default::default(),
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut e: esvc_core::Engine = (*E).clone();
    let mut parts = data
        .split(|&i| i == b':')
        .flat_map(|i| from_utf8(i).ok())
        .filter(|j| !j.is_empty());
    let init_data = parts.next().unwrap_or("");
    let mut w = esvc_core::WorkCache::new(init_data.as_bytes().to_vec());
    let mut sears = Vec::new();
    while let Some(search) = parts.next() {
        if let Some(replacement) = parts.next() {
            sears.push(SearEvent {
                search,
                replacement,
            });
        }
    }

    let expected_result = sears.iter().fold(init_data.to_string(), |acc, item| {
        acc.replace(item.search, item.replacement)
    });

    let x = w
        .shelve_events(
            &mut e,
            Default::default(),
            sears.into_iter().map(|i| i.into()).collect(),
        )
        .expect("unable to shelve events");

    let minx: BTreeSet<_> = e
        .graph()
        .fold_state(x.iter().map(|&y| (y, false)).collect(), false)
        .unwrap()
        .into_iter()
        .map(|x| x.0)
        .collect();

    let x: BTreeSet<_> = x.into_iter().collect();

    let mut tt = Default::default();
    for i in &minx {
        let (_, tt2) = w.run_recursively(&e, tt, *i, true).unwrap();
        tt = tt2;
    }
    assert_eq!(x, tt);
    assert_eq!(from_utf8(&w.0[&x]).unwrap(), &*expected_result);
});
