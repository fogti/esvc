pub use anyhow;

pub trait EngineError: Sized + Sync + Send + Into<anyhow::Error> {}
impl<T: Sync + Send + Into<anyhow::Error>> EngineError for T {}

pub trait Engine: Sync {
    type Command;
    type Error: EngineError;

    /// execute an event of a given data `dat`, ignoring dependencies.
    /// returns `Err` if execution failed, and everything already lookup'ed
    fn run_event_bare(
        &self,
        cmd: &Self::Command,
        arg: &[u8],
        dat: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;

    /// lookup a command in the internal index
    fn resolve_cmd(&self, cmd: u32) -> Option<&Self::Command>;
}
