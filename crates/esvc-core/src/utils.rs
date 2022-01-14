use crate::Hash;
use std::collections::BTreeSet;

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error("dependency not satisfied: {0}")]
    DependencyUnsatisfied(Hash),

    #[error("re-run of event {0} forbidden")]
    RerunForbidden(Hash),
}

/// checks if an `Event` with ID `evid` and dependencies `deps` is applicable
pub fn can_run(
    trackertop: &BTreeSet<Hash>,
    evid: &Hash,
    deps: &BTreeSet<Hash>,
) -> Result<(), ApplyError> {
    if trackertop.contains(evid) {
        // this only catches direct reruns
        return Err(ApplyError::RerunForbidden(*evid));
    }
    if let Some(&x) = deps.difference(trackertop).next() {
        return Err(ApplyError::DependencyUnsatisfied(x));
    }
    Ok(())
}
