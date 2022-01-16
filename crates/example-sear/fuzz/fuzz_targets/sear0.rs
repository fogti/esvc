#![no_main]
use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use once_cell::sync::Lazy;
use std::collections::BTreeSet;
use std::str::from_utf8;

static E: Lazy<esvc_core::Engine> = Lazy::new(|| {
    let mut e = esvc_core::Engine::new().expect("unable to initialize engine");
    e.add_command(
        include_bytes!("../../../../wasm-crates/example-sear/pkg/example_sear_bg.wasm").to_vec(),
    )
    .expect("unable to insert module");
    e
});

#[derive(Clone, Debug)]
struct NonEmptyString(String);

impl core::ops::Deref for NonEmptyString {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        &*self.0
    }
}

impl<'a> From<(char, &'a str)> for NonEmptyString {
    fn from((start, rest): (char, &'a str)) -> Self {
        Self(core::iter::once(start).chain(rest.chars()).collect())
    }
}

impl<'a> Arbitrary<'a> for NonEmptyString {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        u.arbitrary::<(char, &str)>().map(|i| i.into())
    }
    fn arbitrary_take_rest(u: Unstructured<'a>) -> arbitrary::Result<Self> {
        <(char, &str) as Arbitrary>::arbitrary_take_rest(u).map(|i| i.into())
    }
    #[inline]
    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <(char, &str) as Arbitrary>::size_hint(depth)
    }
}

#[derive(Arbitrary, Clone, Debug)]
struct SearEvent {
    search: NonEmptyString,
    replacement: String,
}

impl From<SearEvent> for esvc_core::Event {
    fn from(ev: SearEvent) -> esvc_core::Event {
        esvc_core::Event {
            cmd: 0,
            arg: serde_json::to_string(&serde_json::json!({
                "search": *ev.search,
                "replacement": ev.replacement,
            }))
            .unwrap()
            .into(),
            deps: Default::default(),
        }
    }
}

fuzz_target!(|data: (NonEmptyString, SearEvent, Vec<SearEvent>)| {
    let (init_data, fisear, rsears) = data;
    let mut w = esvc_core::WorkCache::new(init_data.as_bytes().to_vec());
    let sears: Vec<_> = core::iter::once(fisear).chain(rsears.into_iter()).collect();

    let expected_result = sears.iter().fold(init_data.to_string(), |acc, item| {
        acc.replace(&*item.search, &item.replacement)
    });

    let mut e: esvc_core::Engine = (*E).clone();

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
