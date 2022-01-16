#![no_std]
#![forbid(unsafe_code)]

use core::{cmp::PartialEq, fmt::Debug};

pub trait EngineError: Sized + Sync + Send + 'static {}
impl<T: Sync + Send + 'static> EngineError for T {}

pub trait CommandArg: Sized + Debug + Sync + PartialEq + serde::Serialize {}
impl<T: Debug + Sync + PartialEq + serde::Serialize> CommandArg for T {}

pub trait FlowData: Sized + Clone + Sync + Send + PartialEq {}
impl<T: Clone + Sync + Send + PartialEq> FlowData for T {}

pub trait Engine: Sync {
    type Command;
    type Error: EngineError;
    type Arg: CommandArg;
    type Dat: FlowData;

    /// execute an event of a given data `dat`, ignoring dependencies.
    /// returns `Err` if execution failed, and everything already lookup'ed
    fn run_event_bare(
        &self,
        cmd: &Self::Command,
        arg: &Self::Arg,
        dat: &Self::Dat,
    ) -> Result<Self::Dat, Self::Error>;

    /// lookup a command in the internal index
    fn resolve_cmd(&self, cmd: u32) -> Option<&Self::Command>;
}
