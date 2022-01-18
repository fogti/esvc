#![no_main]
use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use std::collections::{BTreeMap, BTreeSet};

struct FuzzEngine;

impl esvc_core::Engine for FuzzEngine {
    type Error = ();
    type Arg = SearEvent;
    type Dat = String;

    fn run_event_bare(&self, cmd: u32, arg: &SearEvent, dat: &String) -> Result<String, ()> {
        assert_eq!(cmd, 0);
        Ok(dat.replace(&*arg.search, &arg.replacement))
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
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

#[derive(Arbitrary, Clone, Debug, PartialEq, serde::Serialize)]
struct SearEvent {
    search: NonEmptyString,
    replacement: String,
}

impl From<SearEvent> for esvc_core::Event<SearEvent> {
    fn from(ev: SearEvent) -> esvc_core::Event<SearEvent> {
        esvc_core::Event {
            cmd: 0,
            arg: ev,
            deps: Default::default(),
        }
    }
}

fuzz_target!(|data: (NonEmptyString, SearEvent, Vec<SearEvent>)| {
    let (init_data, fisear, rsears) = data;
    let sears: Vec<_> = core::iter::once(fisear).chain(rsears.into_iter()).collect();

    let expected_result = sears.iter().fold(init_data.0.clone(), |acc, item| {
        acc.replace(&*item.search, &item.replacement)
    });

    /*
    tracing::subscriber::with_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::stderr)
            .finish(),
        || {
    */
            let e = FuzzEngine;
            let mut g = esvc_core::Graph::default();
            let mut w = esvc_core::WorkCache::new(&e, init_data.0);

            let mut xs = BTreeSet::new();
            for i in sears {
                if let Some(h) = w
                    .shelve_event(&mut g, xs.clone(), i.into())
                    .expect("unable to shelve event")
                {
                    xs.insert(h);
                }
            }

            let minx: BTreeSet<_> = g
                .fold_state(xs.iter().map(|&y| (y, false)).collect(), false)
                .unwrap()
                .into_iter()
                .map(|x| x.0)
                .collect();

            let evs: BTreeMap<_, _> = minx
                .iter()
                .map(|&i| (i, esvc_core::IncludeSpec::IncludeAll))
                .collect();

            let (got, tt) = w.run_foreach_recursively(&g, evs.clone()).unwrap();
            assert_eq!(xs, tt);
            if got != &*expected_result {
                eprintln!("got: {:?}", got);
                eprintln!("exp: {:?}", expected_result);

                println!(":: e.graph.events[] ::");
                for (h, ev) in &g.events {
                    println!("{} {:?}", h, ev.arg);
                    esvc_core::print_deps(&mut std::io::stdout(), ">> ", ev.deps.iter().copied())
                        .unwrap();
                    println!();
                }

                println!("exec order ::");
                esvc_core::print_deps(
                    &mut std::io::stdout(),
                    ">> ",
                    g.calculate_dependencies(evs).unwrap().into_iter(),
                )
                .unwrap();

                panic!("results mismatch");
            }
    /*
        },
    );
    */
});
