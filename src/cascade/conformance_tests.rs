//! Engine ↔ real-git conformance: the model suite's missing half.
//!
//! `model_tests` proves the engine correct against a *symbolic* world whose
//! semantics mirror the interpreter contracts. This suite closes the loop by
//! running the **actual engine loop** against **real git repositories** via
//! [`WorktreeGitInterpreter`], with only GitHub faked ([`FakeGitHub`]:
//! squashes are real squash commits on the bare remote, `RefetchPr` reads
//! the real refs, retargets are bookkeeping). If the model's semantics
//! diverge from real git anywhere the engine cares about — ancestry, ours
//! merges, push idempotency — a cascade driven here fails.
//!
//! Real-git tests are slow: shapes are small and proptest cases few. A
//! failure here should be re-run in isolation before being believed (the
//! real-git harness is occasionally environmentally flaky).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use proptest::prelude::*;

use crate::effects::github::GitHubEffect;
use crate::effects::{Effect, GitHubResponse, PrData, RepoSettingsData};
use crate::git::interpreter::{WorktreeGitInterpreter, classify_git_error};
use crate::git::test_support::{
    create_branch_with_file, create_pr_ref, create_test_repo_with_origin, squash_merge_to_main,
};
use crate::git::{GitConfig, run_git_stdout, run_git_sync};
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::{PersistedRepoSnapshot, SCHEMA_VERSION};
use crate::state::RepoState;
use crate::test_utils::test_timestamp;
use crate::types::{CachedPr, CommentId, MergeStateStatus, PrNumber, PrState, Sha, TrainErrorKind};

use super::plan::{EffectError, EffectOutcome, EffectResponse};
use super::{Control, Observation, ReplayFacts, StepPlan, advance, observe, start_train};

// ─── The GitHub half of the world (the git half is real) ───

#[derive(Debug, Clone, PartialEq, Eq)]
enum FakePrState {
    Open,
    Merged { squash_sha: Sha },
}

#[derive(Debug, Clone)]
struct FakePr {
    branch: String,
    base_ref: String,
    state: FakePrState,
}

struct FakeGitHub {
    config: GitConfig,
    prs: HashMap<PrNumber, FakePr>,
    next_comment: u64,
    squash_count: HashMap<PrNumber, u32>,
}

impl FakeGitHub {
    /// The real head of an open PR's branch on the bare remote.
    fn branch_head(&self, branch: &str) -> Sha {
        let sha = run_git_stdout(
            &self.config.clone_dir(),
            &["rev-parse", &format!("refs/heads/{branch}")],
        )
        .unwrap();
        Sha::parse(sha).unwrap()
    }

    /// GitHub keeps `refs/pull/<n>/head` tracking the PR branch while open
    /// and frozen after merge. The bot's pushes move branches via the real
    /// interpreter, so re-mirror after every effect.
    fn sync_pr_refs(&self) {
        for (number, pr) in &self.prs {
            if matches!(pr.state, FakePrState::Open) {
                let head = self.branch_head(&pr.branch);
                create_pr_ref(&self.config, number.0, &head);
            }
        }
    }

    fn execute(&mut self, effect: &GitHubEffect) -> Result<GitHubResponse, EffectError> {
        match effect {
            GitHubEffect::GetRepoSettings => Ok(GitHubResponse::RepoSettings(RepoSettingsData {
                default_branch: "main".to_string(),
                allow_squash_merge: true,
                allow_merge_commit: false,
                allow_rebase_merge: false,
            })),
            GitHubEffect::GetBranchProtection { .. } => Ok(GitHubResponse::BranchProtectionUnknown),
            GitHubEffect::GetRulesets => Ok(GitHubResponse::RulesetsUnknown),

            GitHubEffect::SquashMerge { pr, expected_sha } => {
                let fake = self.prs.get(pr).expect("squash of a known PR").clone();
                if !matches!(fake.state, FakePrState::Open) {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: format!("PR {pr} is not open"),
                    });
                }
                let head = self.branch_head(&fake.branch);
                if head != *expected_sha {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::HeadShaChanged,
                        detail: format!("expected {expected_sha}, head is {head}"),
                    });
                }
                // A real squash commit on the bare remote's main.
                let squash = squash_merge_to_main(&self.config, expected_sha);
                self.prs.get_mut(pr).unwrap().state = FakePrState::Merged {
                    squash_sha: squash.squash_sha.clone(),
                };
                *self.squash_count.entry(*pr).or_insert(0) += 1;
                Ok(GitHubResponse::Merged {
                    sha: squash.squash_sha,
                })
            }

            GitHubEffect::RetargetPr { pr, new_base } => {
                let fake = self.prs.get_mut(pr).expect("retarget of a known PR");
                if !matches!(fake.state, FakePrState::Open) {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::PrClosed,
                        detail: format!("PR {pr} is not open"),
                    });
                }
                fake.base_ref = new_base.clone();
                Ok(GitHubResponse::Retargeted)
            }

            GitHubEffect::RefetchPr { pr } => {
                let fake = self.prs.get(pr).expect("refetch of a known PR");
                let (state, head_sha, merge_state) = match &fake.state {
                    FakePrState::Open => (
                        PrState::Open,
                        self.branch_head(&fake.branch),
                        MergeStateStatus::Clean,
                    ),
                    FakePrState::Merged { squash_sha } => {
                        // The frozen PR ref names the squashed head.
                        let head = run_git_stdout(
                            &self.config.clone_dir(),
                            &["rev-parse", &format!("refs/pull/{}/head", pr.0)],
                        )
                        .unwrap();
                        (
                            PrState::Merged {
                                merge_commit_sha: squash_sha.clone(),
                            },
                            Sha::parse(head).unwrap(),
                            MergeStateStatus::Unknown,
                        )
                    }
                };
                Ok(GitHubResponse::PrRefetched {
                    pr: PrData {
                        number: *pr,
                        head_sha,
                        head_ref: fake.branch.clone(),
                        base_ref: fake.base_ref.clone(),
                        state,
                        is_draft: false,
                    },
                    merge_state,
                })
            }

            GitHubEffect::PostComment { .. } => {
                let id = CommentId(self.next_comment);
                self.next_comment += 1;
                Ok(GitHubResponse::CommentPosted { id })
            }
            GitHubEffect::UpdateComment { .. } => Ok(GitHubResponse::CommentUpdated),
            GitHubEffect::AddReaction { .. } => Ok(GitHubResponse::ReactionAdded),

            other => panic!("the engine does not emit {other:?}"),
        }
    }
}

// ─── Stack seeding on a real repo ───

/// Same shape vocabulary as the model suite: PR `i+2`'s predecessor is
/// `predecessors[i] + 1`; `advanced[i]` pushes an extra commit to PR `i+1`'s
/// branch after its descendants fork.
#[derive(Debug, Clone)]
struct StackShape {
    predecessors: Vec<usize>,
    advanced: Vec<bool>,
}

fn pr_number(index: usize) -> PrNumber {
    PrNumber(index as u64 + 1)
}

fn branch_name(index: usize) -> String {
    format!("pr-{}", index + 1)
}

fn seed(shape: &StackShape) -> (tempfile::TempDir, GitConfig, FakeGitHub, RepoState) {
    let (temp, config, _initial) = create_test_repo_with_origin();
    let n = shape.predecessors.len() + 1;

    let mut fake_prs = HashMap::new();
    let mut cached = HashMap::new();
    for i in 0..n {
        let number = pr_number(i);
        let (base_branch, predecessor) = if i == 0 {
            ("main".to_string(), None)
        } else {
            let p = shape.predecessors[i - 1];
            (branch_name(p), Some(pr_number(p)))
        };
        let head = create_branch_with_file(
            &config,
            &branch_name(i),
            &format!("pr-{}.txt", i + 1),
            &format!("content of pr-{}", i + 1),
            &base_branch,
        );
        create_pr_ref(&config, number.0, &head);
        fake_prs.insert(
            number,
            FakePr {
                branch: branch_name(i),
                base_ref: base_branch.clone(),
                state: FakePrState::Open,
            },
        );
        cached.insert(
            number,
            CachedPr::new(
                number,
                head,
                branch_name(i),
                base_branch,
                predecessor,
                PrState::Open,
                MergeStateStatus::Unknown,
                false,
            ),
        );
    }

    // Advance the chosen branches after every descendant has forked, so
    // preparation is a real merge followed by a real push.
    for (i, advanced) in shape.advanced.iter().enumerate() {
        if *advanced {
            let head = create_branch_with_file(
                &config,
                &branch_name(i),
                &format!("pr-{}-fix.txt", i + 1),
                "late fix",
                &branch_name(i),
            );
            create_pr_ref(&config, pr_number(i).0, &head);
        }
    }

    let snapshot = PersistedRepoSnapshot {
        schema_version: SCHEMA_VERSION,
        snapshot_at: test_timestamp(),
        log_generation: 0,
        log_position: 0,
        next_seq: 0,
        default_branch: "main".to_string(),
        prs: cached,
        active_trains: HashMap::new(),
        seen_dedupe_keys: HashMap::new(),
    };
    let github = FakeGitHub {
        config: config.clone(),
        prs: fake_prs,
        next_comment: 1,
        squash_count: HashMap::new(),
    };
    (temp, config, github, RepoState::from_snapshot(snapshot))
}

// ─── The driver: M5's loop over the real interpreter ───

struct Driver {
    state: RepoState,
    github: FakeGitHub,
    config: GitConfig,
    log: Vec<StateEvent>,
    seq: u64,
    now: DateTime<Utc>,
    steps: u32,
}

#[derive(Debug, PartialEq, Eq)]
enum RunOutcome {
    Done,
    Parked,
    FanOut(Vec<PrNumber>),
}

impl Driver {
    fn new(config: GitConfig, github: FakeGitHub, state: RepoState) -> Self {
        Driver {
            state,
            github,
            config,
            log: Vec::new(),
            seq: 0,
            now: test_timestamp(),
            steps: 0,
        }
    }

    fn append(&mut self, payloads: &[StateEventPayload]) {
        for payload in payloads {
            let event = StateEvent {
                seq: self.seq,
                ts: self.now,
                payload: payload.clone(),
            };
            self.seq += 1;
            self.state.apply_event(&event);
            self.log.push(event);
        }
    }

    fn execute(
        &mut self,
        interpreter: &WorktreeGitInterpreter,
        effect: &Effect,
    ) -> Result<EffectResponse, EffectError> {
        let result = match effect {
            Effect::Git(git) => interpreter
                .interpret(git)
                .map(EffectResponse::Git)
                .map_err(|e| classify_git_error(&e)),
            Effect::GitHub(gh) => self.github.execute(gh).map(EffectResponse::GitHub),
        };
        // GitHub mirrors branch pushes into the PR refs.
        self.github.sync_pr_refs();
        result
    }

    fn run_plan(&mut self, mut plan: StepPlan, root: PrNumber) -> RunOutcome {
        let interpreter = WorktreeGitInterpreter::new(self.config.clone(), root);
        loop {
            self.steps += 1;
            assert!(self.steps < 500, "cascade did not terminate");

            let events = plan.events.clone();
            self.append(&events);

            let mut outcomes = Vec::new();
            for effect in &plan.effects {
                let result = self.execute(&interpreter, effect);
                let failed = result.is_err();
                outcomes.push(EffectOutcome {
                    effect: effect.clone(),
                    result,
                });
                if failed {
                    break;
                }
            }
            for effect in &plan.best_effort {
                let _ = self.execute(&interpreter, effect);
            }

            match plan.control {
                Control::Done => return RunOutcome::Done,
                Control::Park => return RunOutcome::Parked,
                Control::FanOut { new_roots } => return RunOutcome::FanOut(new_roots),
                Control::Continue => {
                    let obs = observe(&outcomes).expect("plan outcomes must be observable");
                    plan = advance(&self.state, root, obs, self.now)
                        .expect("advance must accept its own observations");
                }
            }
        }
    }

    fn run_to_completion(&mut self, root: PrNumber) {
        let plan = start_train(&self.state, root, self.now).expect("start plans");
        let mut queue = vec![(root, Some(plan))];
        let mut rounds = 0;
        while let Some((r, plan)) = queue.pop() {
            rounds += 1;
            assert!(rounds < 100, "train queue did not drain");
            let plan = match plan {
                Some(p) => p,
                None => {
                    if !self
                        .state
                        .active_trains
                        .get(&r)
                        .is_some_and(|t| t.state.is_active())
                    {
                        continue;
                    }
                    let facts = ReplayFacts::for_train(&self.log, r);
                    advance(&self.state, r, Observation::Evaluate { facts }, self.now)
                        .expect("evaluate plans")
                }
            };
            match self.run_plan(plan, r) {
                RunOutcome::Done => {}
                RunOutcome::Parked => queue.push((r, None)),
                RunOutcome::FanOut(new_roots) => {
                    for nr in new_roots {
                        queue.push((nr, None));
                    }
                }
            }
        }
    }
}

// ─── End-state assertions against the REAL repository ───

fn assert_conformance(driver: &Driver, shape: &StackShape) {
    let clone_dir = driver.config.clone_dir();
    let n = shape.predecessors.len() + 1;

    // Every PR merged exactly once; every train gone.
    for i in 0..n {
        let number = pr_number(i);
        assert!(
            matches!(driver.github.prs[&number].state, FakePrState::Merged { .. }),
            "PR {number} did not merge"
        );
        assert_eq!(
            driver.github.squash_count.get(&number),
            Some(&1),
            "PR {number} must be squashed exactly once"
        );
        assert!(driver.state.prs[&number].state.is_merged());
    }
    assert!(driver.state.active_trains.is_empty());

    // All content — including post-fork fixes that travelled through real
    // preparation merges — reached main's tree.
    let main_tree =
        run_git_stdout(&clone_dir, &["ls-tree", "--name-only", "refs/heads/main"]).unwrap();
    for i in 0..n {
        assert!(
            main_tree.contains(&format!("pr-{}.txt", i + 1)),
            "pr-{}'s content missing from main: {main_tree}",
            i + 1
        );
        if shape.advanced.get(i) == Some(&true) {
            assert!(
                main_tree.contains(&format!("pr-{}-fix.txt", i + 1)),
                "pr-{}'s post-fork fix missing from main",
                i + 1
            );
        }
    }

    // Real-git reconciliation evidence: each descendant's final branch head
    // has its predecessor's squash commit as an ancestor (the ours-merge
    // dance actually happened on the real repo), and the recorded marker
    // matches the real squash SHA.
    for i in 1..n {
        let number = pr_number(i);
        let pred = pr_number(shape.predecessors[i - 1]);
        let FakePrState::Merged { squash_sha } = &driver.github.prs[&pred].state else {
            panic!("predecessor {pred} not merged");
        };
        let ancestor_check = run_git_sync(
            &clone_dir,
            &[
                "merge-base",
                "--is-ancestor",
                squash_sha.as_str(),
                &format!("refs/pull/{}/head", number.0),
            ],
        );
        assert!(
            ancestor_check.is_ok(),
            "PR {number}'s squashed head must contain predecessor {pred}'s squash {squash_sha}"
        );
        assert_eq!(
            driver.state.prs[&number]
                .predecessor_squash_reconciled
                .as_ref(),
            Some(squash_sha),
            "PR {number}'s marker must match the real squash"
        );
        assert_eq!(driver.github.prs[&number].base_ref, "main");
    }

    // No dangling intents.
    for i in 0..n {
        let facts = ReplayFacts::for_train(&driver.log, pr_number(i));
        assert_eq!(facts.unmatched().count(), 0);
    }
}

fn run_shape(shape: StackShape) {
    let (_temp, config, github, state) = seed(&shape);
    let mut driver = Driver::new(config, github, state);
    driver.run_to_completion(pr_number(0));
    assert_conformance(&driver, &shape);
}

// ─── The tests ───

/// A linear chain with an advanced root: real preparation merges, a real
/// squash, real reconcile/catch-up, ancestry verified with real git.
#[test]
fn linear_chain_conforms() {
    run_shape(StackShape {
        predecessors: vec![0, 1],
        advanced: vec![true, false, false],
    });
}

/// Fan-out: two descendants prepared against the same pin, both reconciled
/// against the same real squash, then driven as independent trains.
#[test]
fn fan_out_conforms() {
    run_shape(StackShape {
        predecessors: vec![0, 0],
        advanced: vec![true, false, false],
    });
}

/// Every PR advanced after its descendants forked: every phase does real
/// work (no already-up-to-date shortcuts anywhere).
#[test]
fn fully_advanced_chain_conforms() {
    run_shape(StackShape {
        predecessors: vec![0],
        advanced: vec![true, true],
    });
}

proptest! {
    // Real git is slow: few cases, small shapes. This is the fidelity check
    // for the model suite's semantics, not a coverage engine (model_tests
    // covers the state space).
    #![proptest_config(ProptestConfig {
        cases: 6,
        ..ProptestConfig::default()
    })]

    #[test]
    fn generated_stacks_conform(
        choices in prop::collection::vec(any::<prop::sample::Index>(), 0..=3),
        advanced_bits in prop::collection::vec(any::<bool>(), 4),
    ) {
        let predecessors: Vec<usize> = choices
            .iter()
            .enumerate()
            .map(|(i, idx)| idx.index(i + 1))
            .collect();
        let mut advanced = advanced_bits;
        advanced.truncate(predecessors.len() + 1);
        run_shape(StackShape {
            predecessors,
            advanced,
        });
    }
}
