// SPDX-License-Identifier: MIT OR Apache-2.0
// large parts of this were taken from `petgraph`
// ref = https://github.com/petgraph/petgraph/blob/9ff688872b467d3e1b5adef19f5c52f519d3279c/src/dot.rs

use crate::Graph;
use core::fmt::{self, Formatter, Result, Write};

/// A formatter which can format a graph into the .dot format,
/// useful for debugging and visualization
pub struct Dot<'a, Arg>(pub &'a Graph<Arg>);

impl<Arg> Dot<'_, Arg> {
    fn graph_fmt<AF>(&self, f: &mut Formatter<'_>, argfmtf: AF) -> Result
    where
        AF: Fn(&Arg, &mut Formatter<'_>) -> Result,
    {
        writeln!(f, "digraph {{")?;

        // labels
        for (h, i) in &self.0.events {
            writeln!(
                f,
                "  \"{h}\" [label=\"{h}\\n{}:{}\"];",
                i.cmd,
                Escaped(FnFmt(&i.arg, &argfmtf)),
                h = h,
            )?;
        }

        // edges
        for (h, i) in &self.0.events {
            for dep in &i.deps {
                writeln!(f, "  \"{}\" -> \"{}\";", h, dep)?;
            }
        }

        // clusters
        for (nstate, deps) in &self.0.nstates {
            writeln!(f, "  subgraph \"cluster_{}\" {{", Escaped(&nstate))?;
            for dep in deps {
                writeln!(f, "  \"{}\";", dep)?;
            }
            writeln!(f, "  }}")?;
        }

        writeln!(f, "}}")
    }
}

impl<Arg: fmt::Display> fmt::Display for Dot<'_, Arg> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.graph_fmt(f, fmt::Display::fmt)
    }
}

impl<Arg: fmt::Debug> fmt::Debug for Dot<'_, Arg> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.graph_fmt(f, fmt::Debug::fmt)
    }
}

/// Escape for Graphviz
struct Escaper<W>(W);

impl<W> fmt::Write for Escaper<W>
where
    W: fmt::Write,
{
    fn write_str(&mut self, s: &str) -> Result {
        for c in s.chars() {
            self.write_char(c)?;
        }
        Ok(())
    }

    fn write_char(&mut self, c: char) -> Result {
        match c {
            '"' | '\\' => self.0.write_char('\\')?,
            // \l is for left justified linebreak
            '\n' => return self.0.write_str("\\l"),
            _ => {}
        }
        self.0.write_char(c)
    }
}

/// Pass Display formatting through a simple escaping filter
struct Escaped<T>(T);

impl<T> fmt::Display for Escaped<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut Formatter) -> Result {
        if f.alternate() {
            writeln!(&mut Escaper(f), "{:#}", &self.0)
        } else {
            write!(&mut Escaper(f), "{}", &self.0)
        }
    }
}

/// Format data using a specific format function
struct FnFmt<'a, T, F>(&'a T, F);

impl<'a, T, F> fmt::Display for FnFmt<'a, T, F>
where
    F: Fn(&'a T, &mut Formatter<'_>) -> Result,
{
    fn fmt(&self, f: &mut Formatter) -> Result {
        self.1(self.0, f)
    }
}
