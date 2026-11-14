//! One process invocation of a terminal pane, as the screen sees it.
//!
//! Output, a deferred exit, and a later start are matched by generation.
//! An exit waits until the reader drains so the last bytes are parsed
//! before the pane closes or holds. Pending-event admission caps live
//! elsewhere; this module only decides which run an instruction belongs to.

use std::collections::HashMap;

use super::ScreenInstruction;

/// One process invocation of a terminal pane.
pub(super) struct TerminalRun {
    pub(super) generation: u64,
    /// The reader has sent `PtyRunDrained` for this generation.
    pub(super) drained: bool,
    /// Child exit waiting until the reader drains, so the last bytes are
    /// parsed before the pane closes or holds.
    pub(super) deferred_exit: Option<ScreenInstruction>,
}

/// Record `generation` as the terminal's current run when nothing is recorded.
/// Returns false when a different run is current: the caller drops the bytes.
pub(super) fn run_generation_accepts(
    runs: &mut HashMap<u32, TerminalRun>,
    terminal_id: u32,
    generation: u64,
) -> bool {
    match runs.get(&terminal_id) {
        Some(run) if run.generation != generation => {
            if generation > run.generation {
                // The new process can produce output before the screen sees
                // its `TerminalRunStarted`; accept it as the current run.
                runs.insert(
                    terminal_id,
                    TerminalRun {
                        generation,
                        drained: false,
                        deferred_exit: None,
                    },
                );
                true
            } else {
                false
            }
        },
        Some(_) => true,
        None => {
            runs.insert(
                terminal_id,
                TerminalRun {
                    generation,
                    drained: false,
                    deferred_exit: None,
                },
            );
            true
        },
    }
}

/// Whether a lifecycle instruction for `generation` still applies. An
/// unknown run is accepted: a child can exit before the screen has seen its
/// `TerminalRunStarted`, and that exit still has to settle the pane.
pub(super) fn run_generation_matches(
    runs: &HashMap<u32, TerminalRun>,
    terminal_id: u32,
    generation: u64,
) -> bool {
    runs.get(&terminal_id)
        .is_none_or(|run| run.generation == generation)
}

/// Hold a child exit until its run has drained. Returns the instruction when
/// it should be applied now.
pub(super) fn stage_child_exit(
    runs: &mut HashMap<u32, TerminalRun>,
    terminal_id: u32,
    generation: u64,
    instruction: ScreenInstruction,
) -> Option<ScreenInstruction> {
    match runs.get(&terminal_id).map(|run| run.generation) {
        Some(current) if current != generation => {
            if generation > current {
                // A restarted process can exit before its `TerminalRunStarted`.
                // Remember the exit for the generation that is about to start.
                runs.insert(
                    terminal_id,
                    TerminalRun {
                        generation,
                        drained: false,
                        deferred_exit: Some(instruction),
                    },
                );
            }
            None
        },
        Some(_) => {
            let run = runs.get_mut(&terminal_id).expect("run exists");
            if run.drained {
                Some(instruction)
            } else {
                run.deferred_exit = Some(instruction);
                None
            }
        },
        None => {
            runs.insert(
                terminal_id,
                TerminalRun {
                    generation,
                    drained: false,
                    deferred_exit: Some(instruction),
                },
            );
            None
        },
    }
}

/// Mark the run drained and take a deferred exit, if this generation is current.
pub(super) fn stage_run_drained(
    runs: &mut HashMap<u32, TerminalRun>,
    terminal_id: u32,
    generation: u64,
) -> Option<ScreenInstruction> {
    let run = runs.get_mut(&terminal_id)?;
    if run.generation != generation {
        return None;
    }
    run.drained = true;
    run.deferred_exit.take()
}

/// A new process replaces the previous run. A deferred exit of the previous
/// run is dropped: it must not close the process that just started.
///
/// A start for the same generation does not replace the record. An immediate
/// exit can arrive first and leave a deferred exit that this start must keep.
/// A later start for an older generation must not replace a newer run.
pub(super) fn stage_run_started(
    runs: &mut HashMap<u32, TerminalRun>,
    terminal_id: u32,
    generation: u64,
) {
    if let Some(run) = runs.get(&terminal_id) {
        if run.generation >= generation {
            return;
        }
    }
    runs.insert(
        terminal_id,
        TerminalRun {
            generation,
            drained: false,
            deferred_exit: None,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::panes::PaneId;

    fn child_close(terminal_id: u32, generation: u64) -> ScreenInstruction {
        ScreenInstruction::ChildExitClose(PaneId::Terminal(terminal_id), Some(0), generation)
    }

    #[test]
    fn a_child_exit_waits_for_the_reader_to_drain() {
        let mut runs = HashMap::new();
        let held = stage_child_exit(&mut runs, 4, 1, child_close(4, 1));
        assert!(held.is_none());
        assert!(stage_run_drained(&mut runs, 4, 1).is_some());
    }

    #[test]
    fn an_exit_after_drain_is_applied_immediately() {
        let mut runs = HashMap::new();
        runs.insert(
            4,
            TerminalRun {
                generation: 1,
                drained: true,
                deferred_exit: None,
            },
        );
        assert!(stage_child_exit(&mut runs, 4, 1, child_close(4, 1)).is_some());
    }

    #[test]
    fn an_immediate_exit_before_start_keeps_its_deferred_exit() {
        let mut runs = HashMap::new();
        assert!(stage_child_exit(&mut runs, 4, 1, child_close(4, 1)).is_none());
        stage_run_started(&mut runs, 4, 1);
        assert!(stage_run_drained(&mut runs, 4, 1).is_some());
    }

    #[test]
    fn a_new_run_drops_the_previous_runs_exit_and_bytes() {
        let mut runs = HashMap::new();
        assert!(stage_child_exit(&mut runs, 4, 1, child_close(4, 1)).is_none());
        stage_run_started(&mut runs, 4, 2);
        assert!(stage_run_drained(&mut runs, 4, 1).is_none());
        assert!(stage_child_exit(&mut runs, 4, 1, child_close(4, 1)).is_none());
        assert!(!run_generation_accepts(&mut runs, 4, 1));
        assert!(run_generation_accepts(&mut runs, 4, 2));
    }

    #[test]
    fn an_older_start_does_not_replace_a_newer_run() {
        let mut runs = HashMap::new();
        stage_run_started(&mut runs, 4, 2);
        stage_run_started(&mut runs, 4, 1);
        assert!(run_generation_matches(&runs, 4, 2));
        assert!(!run_generation_matches(&runs, 4, 1));
    }

    #[test]
    fn an_unknown_generation_is_accepted_for_exit_settlement() {
        let runs = HashMap::new();
        assert!(run_generation_matches(&runs, 4, 1));
    }

    #[test]
    fn a_settlement_for_a_replaced_run_no_longer_matches() {
        let mut runs = HashMap::new();
        assert!(run_generation_matches(&runs, 4, 1));
        stage_run_started(&mut runs, 4, 2);
        assert!(!run_generation_matches(&runs, 4, 1));
        assert!(run_generation_matches(&runs, 4, 2));
    }

    #[test]
    fn held_and_nonheld_exits_both_wait_until_drain() {
        let mut held_runs = HashMap::new();
        let hold = ScreenInstruction::ChildExitHold(
            PaneId::Terminal(4),
            Some(1),
            zellij_utils::input::command::RunCommand::default(),
            3,
        );
        assert!(stage_child_exit(&mut held_runs, 4, 3, hold).is_none());
        assert!(matches!(
            stage_run_drained(&mut held_runs, 4, 3),
            Some(ScreenInstruction::ChildExitHold(..))
        ));

        let mut closed_runs = HashMap::new();
        assert!(stage_child_exit(&mut closed_runs, 4, 3, child_close(4, 3)).is_none());
        assert!(matches!(
            stage_run_drained(&mut closed_runs, 4, 3),
            Some(ScreenInstruction::ChildExitClose(..))
        ));
    }
}
