//! Model-based property tests for the cascade engine — the stage's
//! correctness oracle.
//!
//! [`ModelWorld`] is an in-memory git + GitHub honoring the *interpreter
//! semantics* the real effects have (M4's contracts): `SquashMerge` succeeds
//! iff `expected_sha` matches, pushes reject on foreign advance,
//! `MergeReconcile` refuses without the full ancestry proof,
//! `CheckPushCompleted` compares trees and parent chains. The [`Driver`]
//! runs the executor loop M5 will run (append events → execute effects →
//! `observe` → `advance`), checking the durability bracketing at every
//! irreversible operation.
//!
//! Commits are symbolic: a commit is a parent list plus a set of content
//! "atoms"; a merge unions atoms. Content equality (atom sets) is the
//! cross-run comparison — commit SHAs are not reproducible across recovery,
//! exactly as in real git.

use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use proptest::prelude::*;

use crate::effects::github::GitHubEffect;
use crate::effects::{
    Effect, GitEffect, GitHubResponse, GitResponse, MergeOutcome, PrData, PushOutcome, PushPoint,
    RepoSettingsData,
};
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::{PersistedRepoSnapshot, SCHEMA_VERSION};
use crate::state::RepoState;
use crate::state::transitions::verify_transition_invariants;
use crate::test_utils::test_timestamp;
use crate::types::{
    CachedPr, CascadePhase, CommentId, MergeStateStatus, PhaseKind, PrNumber, PrState, Sha,
    TrainErrorKind, TrainState,
};

use super::plan::{EffectError, EffectOutcome, EffectResponse};
use super::{
    Control, Observation, ReplayFacts, StepPlan, advance, observe, start_train, stop_train,
};

// ─── The model world ───

#[derive(Debug, Clone, PartialEq, Eq)]
struct MCommit {
    parents: Vec<Sha>,
    atoms: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelPrState {
    Open,
    Merged { sha: Sha },
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelPr {
    branch: String,
    base_ref: String,
    state: ModelPrState,
}

#[derive(Debug, Clone)]
struct ModelWorld {
    commits: HashMap<Sha, MCommit>,
    /// Remote branch refs.
    branches: HashMap<String, Sha>,
    /// `refs/pull/<n>/head`: tracks the PR branch while open, frozen at merge.
    pr_refs: HashMap<PrNumber, Sha>,
    prs: HashMap<PrNumber, ModelPr>,
    default_branch: String,
    /// The single stack worktree's HEAD (set by merge effects, read by Push).
    worktree_head: Option<Sha>,
    next_commit: u64,
    next_comment: u64,
    /// What `RefetchPr` reports; defaults to `Clean` for open PRs.
    merge_states: HashMap<PrNumber, MergeStateStatus>,
    /// If armed, the next merge-performing effect conflicts.
    conflict_armed: Option<Vec<String>>,
    /// Repo settings served to the preflight.
    settings: RepoSettingsData,

    // Audit trail for the properties.
    /// Accepted squash merges per PR (must never exceed 1).
    squash_count: HashMap<PrNumber, u32>,
    /// Total irreversible operations (pushes + squashes + retargets).
    irreversible_ops: u32,
    comments: Vec<(PrNumber, String)>,
}

impl ModelWorld {
    fn new() -> Self {
        let mut world = ModelWorld {
            commits: HashMap::new(),
            branches: HashMap::new(),
            pr_refs: HashMap::new(),
            prs: HashMap::new(),
            default_branch: "main".to_string(),
            worktree_head: None,
            next_commit: 0,
            next_comment: 1,
            merge_states: HashMap::new(),
            conflict_armed: None,
            settings: RepoSettingsData {
                default_branch: "main".to_string(),
                allow_squash_merge: true,
                allow_merge_commit: false,
                allow_rebase_merge: false,
            },
            squash_count: HashMap::new(),
            irreversible_ops: 0,
            comments: Vec::new(),
        };
        let base = world.commit(vec![], ["base"]);
        world.branches.insert("main".to_string(), base);
        world
    }

    fn commit(
        &mut self,
        parents: Vec<Sha>,
        extra_atoms: impl IntoIterator<Item = impl Into<String>>,
    ) -> Sha {
        let mut atoms: BTreeSet<String> = BTreeSet::new();
        for p in &parents {
            atoms.extend(self.commits[p].atoms.iter().cloned());
        }
        atoms.extend(extra_atoms.into_iter().map(Into::into));
        self.raw_commit(parents, atoms)
    }

    /// A commit with exactly the given atoms (no parent union) — the
    /// ours-merge shape.
    fn raw_commit(&mut self, parents: Vec<Sha>, atoms: BTreeSet<String>) -> Sha {
        let sha = Sha::parse(format!("{:040x}", self.next_commit)).unwrap();
        self.next_commit += 1;
        self.commits.insert(sha.clone(), MCommit { parents, atoms });
        sha
    }

    fn atoms(&self, sha: &Sha) -> &BTreeSet<String> {
        &self.commits[sha].atoms
    }

    /// The deterministic "tree SHA" of a commit: a function of its atom set
    /// only (same content = same tree, merge metadata irrelevant) — the model
    /// analogue of git's tree object.
    fn tree_sha(&self, commit: &Sha) -> Sha {
        let mut hasher = std::hash::DefaultHasher::new();
        self.atoms(commit).hash(&mut hasher);
        let h = hasher.finish();
        Sha::parse(format!("{h:040x}")).unwrap()
    }

    fn is_ancestor(&self, ancestor: &Sha, descendant: &Sha) -> bool {
        let mut queue = vec![descendant.clone()];
        let mut seen = BTreeSet::new();
        while let Some(c) = queue.pop() {
            if c == *ancestor {
                return true;
            }
            if !seen.insert(c.as_str().to_string()) {
                continue;
            }
            if let Some(commit) = self.commits.get(&c) {
                queue.extend(commit.parents.iter().cloned());
            }
        }
        false
    }

    fn head(&self, branch: &str) -> Option<Sha> {
        self.branches.get(branch).cloned()
    }

    fn pr_with_branch(&self, branch: &str) -> Option<PrNumber> {
        self.prs
            .iter()
            .find(|(_, p)| p.branch == branch)
            .map(|(n, _)| *n)
    }

    /// A merge of `target` into the branch's head, honoring an armed
    /// conflict. Updates the worktree HEAD. Returns the outcome plus the push
    /// point (present iff a new commit was made).
    fn merge_into_branch(
        &mut self,
        branch: &str,
        target: &Sha,
    ) -> Result<(MergeOutcome, Option<PushPoint>), EffectError> {
        let head = self.head(branch).ok_or(EffectError::BranchGone {
            branch: branch.to_string(),
        })?;
        if self.is_ancestor(target, &head) {
            self.worktree_head = Some(head);
            return Ok((MergeOutcome::AlreadyUpToDate, None));
        }
        if let Some(files) = self.conflict_armed.take() {
            self.worktree_head = Some(head);
            return Ok((
                MergeOutcome::Conflict {
                    conflicting_files: files,
                },
                None,
            ));
        }
        let merged = self.commit(vec![head.clone(), target.clone()], Vec::<String>::new());
        self.worktree_head = Some(merged.clone());
        let push_point = PushPoint {
            expected_tree: self.tree_sha(&merged),
            pre_push_sha: Some(head),
        };
        Ok((
            MergeOutcome::Success { commit_sha: merged },
            Some(push_point),
        ))
    }

    fn execute(&mut self, effect: &Effect) -> Result<EffectResponse, EffectError> {
        match effect {
            Effect::Git(git) => self.execute_git(git).map(EffectResponse::Git),
            Effect::GitHub(gh) => self.execute_github(gh).map(EffectResponse::GitHub),
        }
    }

    fn execute_git(&mut self, effect: &GitEffect) -> Result<GitResponse, EffectError> {
        match effect {
            GitEffect::CreateWorktree { .. }
            | GitEffect::RemoveWorktree { .. }
            | GitEffect::CleanupWorktree
            | GitEffect::Fetch { .. } => Ok(GitResponse::Ok),

            GitEffect::RevParse { rev } => {
                let sha_str = rev.strip_suffix('^').expect("engine only parses <sha>^");
                let sha = Sha::parse(sha_str).expect("engine parses a sha it observed");
                let parent = self.commits[&sha].parents.first().cloned().ok_or_else(|| {
                    EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: "commit has no parent".to_string(),
                    }
                })?;
                Ok(GitResponse::Sha(parent))
            }

            GitEffect::PrepareDescendant {
                descendant_branch,
                predecessor_pr,
            } => {
                // The PR ref names the exact predecessor commit (the pin).
                let pin = self.pr_refs[predecessor_pr].clone();
                let (merge, push_point) = self.merge_into_branch(descendant_branch, &pin)?;
                Ok(GitResponse::Prepared {
                    merge,
                    predecessor_head: pin,
                    push_point,
                })
            }

            GitEffect::MergeReconcile {
                descendant_branch,
                predecessor_pr,
                squash_sha,
                expected_squash_parent,
                predecessor_pre_squash_head,
                default_branch,
            } => {
                let desc_head = self
                    .head(descendant_branch)
                    .ok_or(EffectError::BranchGone {
                        branch: descendant_branch.clone(),
                    })?;
                // Guard: the PR ref (frozen at merge) must name the pin.
                let squashed_head = self.pr_refs[predecessor_pr].clone();
                if squashed_head != *predecessor_pre_squash_head {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::HeadShaChanged,
                        detail: "predecessor head changed between prepare and squash".to_string(),
                    });
                }
                // Guard: preparation ancestry.
                if !self.commits.contains_key(predecessor_pre_squash_head)
                    || !self.is_ancestor(predecessor_pre_squash_head, &desc_head)
                {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::PreparationMissing,
                        detail: "descendant does not contain the prepared head".to_string(),
                    });
                }
                // Guards: squash shape.
                let parents = self.commits[squash_sha].parents.clone();
                let main_head = self.head(default_branch).expect("default branch exists");
                let squash_parent = match parents.as_slice() {
                    [only] => only.clone(),
                    _ => {
                        return Err(EffectError::Permanent {
                            kind: TrainErrorKind::NonSquashMerge,
                            detail: "squash commit does not have exactly one parent".to_string(),
                        });
                    }
                };
                if !self.is_ancestor(squash_sha, &main_head)
                    || !self.is_ancestor(&squash_parent, &main_head)
                    || squash_parent != *expected_squash_parent
                {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::NonSquashMerge,
                        detail: "squash parent validation failed".to_string(),
                    });
                }

                // Step 1: merge $SQUASH^; step 2: ours-merge the squash.
                if self.is_ancestor(squash_sha, &desc_head) {
                    self.worktree_head = Some(desc_head);
                    return Ok(GitResponse::MergedForPush {
                        merge: MergeOutcome::AlreadyUpToDate,
                        push_point: None,
                    });
                }
                let (step1, _) = self.merge_into_branch(descendant_branch, &squash_parent)?;
                if let MergeOutcome::Conflict { .. } = step1 {
                    return Ok(GitResponse::MergedForPush {
                        merge: step1,
                        push_point: None,
                    });
                }
                let step1_head = self.worktree_head.clone().expect("merge set HEAD");
                let atoms = self.atoms(&step1_head).clone();
                let ours = self.raw_commit(vec![step1_head, squash_sha.clone()], atoms);
                self.worktree_head = Some(ours.clone());
                let push_point = PushPoint {
                    expected_tree: self.tree_sha(&ours),
                    pre_push_sha: Some(desc_head),
                };
                Ok(GitResponse::MergedForPush {
                    merge: MergeOutcome::Success { commit_sha: ours },
                    push_point: Some(push_point),
                })
            }

            GitEffect::CatchUpDescendant {
                descendant_branch,
                default_branch,
            } => {
                let main_head = self.head(default_branch).expect("default branch exists");
                let (merge, push_point) = self.merge_into_branch(descendant_branch, &main_head)?;
                Ok(GitResponse::MergedForPush { merge, push_point })
            }

            GitEffect::UpdateRootForBehind {
                branch,
                default_branch,
            } => {
                let main_head = self.head(default_branch).expect("default branch exists");
                let (merge, push_point) = self.merge_into_branch(branch, &main_head)?;
                Ok(GitResponse::MergedForPush { merge, push_point })
            }

            GitEffect::Push { refspec, .. } => {
                let branch = refspec
                    .strip_prefix("HEAD:refs/heads/")
                    .expect("engine pushes HEAD:refs/heads/<branch>");
                let head = self.worktree_head.clone().expect("push follows a merge");
                let outcome = match self.head(branch) {
                    Some(remote) if remote == head => PushOutcome::AlreadyUpToDate,
                    Some(remote) if self.is_ancestor(&remote, &head) => {
                        self.record_push(branch, &head);
                        PushOutcome::Pushed { pushed_sha: head }
                    }
                    Some(_) => PushOutcome::Rejected {
                        details: "non-fast-forward".to_string(),
                    },
                    None => {
                        self.record_push(branch, &head);
                        PushOutcome::Pushed { pushed_sha: head }
                    }
                };
                Ok(GitResponse::Push(outcome))
            }

            GitEffect::CheckPushCompleted {
                branch,
                expected_tree,
                pre_push_sha,
                second_parent,
            } => {
                let Some(remote) = self.head(branch) else {
                    return Ok(GitResponse::PushCheck {
                        completed: false,
                        remote_head: None,
                    });
                };
                let check = |completed: bool| GitResponse::PushCheck {
                    completed,
                    remote_head: Some(remote.clone()),
                };
                if self.tree_sha(&remote) != *expected_tree
                    || !self.is_ancestor(pre_push_sha, &remote)
                {
                    return Ok(check(false));
                }
                // Ours-merge case: the tree didn't change, identify our commit
                // by its parents.
                if *expected_tree == self.tree_sha(pre_push_sha) {
                    let parents = &self.commits[&remote].parents;
                    let ok = parents.first() == Some(pre_push_sha)
                        && second_parent
                            .as_ref()
                            .is_none_or(|p2| parents.get(1) == Some(p2));
                    return Ok(check(ok));
                }
                Ok(check(true))
            }

            GitEffect::ValidateSquashCommit {
                squash_sha,
                default_branch,
            } => {
                let main_head = self.head(default_branch).expect("default branch exists");
                let valid = match self.commits.get(squash_sha).map(|c| c.parents.as_slice()) {
                    Some([only]) => {
                        self.is_ancestor(squash_sha, &main_head)
                            && self.is_ancestor(only, &main_head)
                    }
                    _ => false,
                };
                Ok(GitResponse::Bool(valid))
            }

            other => panic!("the engine does not emit {other:?}"),
        }
    }

    fn execute_github(&mut self, effect: &GitHubEffect) -> Result<GitHubResponse, EffectError> {
        match effect {
            GitHubEffect::GetRepoSettings => {
                Ok(GitHubResponse::RepoSettings(self.settings.clone()))
            }
            GitHubEffect::GetBranchProtection { .. } => Ok(GitHubResponse::BranchProtectionUnknown),
            GitHubEffect::GetRulesets => Ok(GitHubResponse::RulesetsUnknown),

            GitHubEffect::SquashMerge { pr, expected_sha } => {
                let model_pr = self.prs.get(pr).expect("squash of a known PR").clone();
                if !matches!(model_pr.state, ModelPrState::Open) {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: format!("PR {pr} is not open"),
                    });
                }
                let head = self.head(&model_pr.branch).expect("open PR has a branch");
                if head != *expected_sha {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::HeadShaChanged,
                        detail: format!("expected {expected_sha}, head is {head}"),
                    });
                }
                let main = self.default_branch.clone();
                let main_head = self.head(&main).unwrap();
                let head_atoms = self.atoms(&head).clone();
                let squash = self.commit(vec![main_head], head_atoms);
                self.branches.insert(main, squash.clone());
                self.pr_refs.insert(*pr, head);
                self.prs.get_mut(pr).unwrap().state = ModelPrState::Merged {
                    sha: squash.clone(),
                };
                *self.squash_count.entry(*pr).or_insert(0) += 1;
                self.irreversible_ops += 1;
                Ok(GitHubResponse::Merged { sha: squash })
            }

            GitHubEffect::RetargetPr { pr, new_base } => {
                let model_pr = self.prs.get_mut(pr).expect("retarget of a known PR");
                if !matches!(model_pr.state, ModelPrState::Open) {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::PrClosed,
                        detail: format!("PR {pr} is not open"),
                    });
                }
                model_pr.base_ref = new_base.clone();
                self.irreversible_ops += 1;
                Ok(GitHubResponse::Retargeted)
            }

            GitHubEffect::RefetchPr { pr } => {
                let model_pr = self.prs.get(pr).expect("refetch of a known PR");
                let (state, head_sha) = match &model_pr.state {
                    ModelPrState::Open => (
                        PrState::Open,
                        self.head(&model_pr.branch).expect("open PR has a branch"),
                    ),
                    ModelPrState::Merged { sha } => (
                        PrState::Merged {
                            merge_commit_sha: sha.clone(),
                        },
                        self.pr_refs[pr].clone(),
                    ),
                    ModelPrState::Closed => (PrState::Closed, self.pr_refs[pr].clone()),
                };
                let merge_state = self.merge_states.get(pr).copied().unwrap_or({
                    if matches!(model_pr.state, ModelPrState::Open) {
                        MergeStateStatus::Clean
                    } else {
                        MergeStateStatus::Unknown
                    }
                });
                Ok(GitHubResponse::PrRefetched {
                    pr: PrData {
                        number: *pr,
                        head_sha,
                        head_ref: model_pr.branch.clone(),
                        base_ref: model_pr.base_ref.clone(),
                        state,
                        is_draft: false,
                    },
                    merge_state,
                })
            }

            GitHubEffect::PostComment { pr, body } => {
                self.comments.push((*pr, body.clone()));
                let id = CommentId(self.next_comment);
                self.next_comment += 1;
                Ok(GitHubResponse::CommentPosted { id })
            }
            GitHubEffect::UpdateComment { .. } => Ok(GitHubResponse::CommentUpdated),
            GitHubEffect::AddReaction { .. } => Ok(GitHubResponse::ReactionAdded),

            other => panic!("the engine does not emit {other:?}"),
        }
    }

    /// A human squash-merges the PR through the GitHub UI (not the bot: no
    /// audit count, no expected-sha fencing).
    fn external_squash(&mut self, pr: PrNumber) {
        let model_pr = self.prs[&pr].clone();
        let head = self.head(&model_pr.branch).expect("open PR has a branch");
        let main = self.default_branch.clone();
        let main_head = self.head(&main).unwrap();
        let head_atoms = self.atoms(&head).clone();
        let squash = self.commit(vec![main_head], head_atoms);
        self.branches.insert(main, squash.clone());
        self.pr_refs.insert(pr, head);
        self.prs.get_mut(&pr).unwrap().state = ModelPrState::Merged { sha: squash };
    }

    /// A fast-forward push by someone else (adds a commit to the branch).
    fn foreign_push(&mut self, branch: &str, atom: &str) {
        let head = self.head(branch).expect("foreign push to a live branch");
        let new = self.commit(vec![head], [atom]);
        self.move_branch(branch, &new);
    }

    /// A force-push replacing the branch with unrelated history.
    fn force_push(&mut self, branch: &str, atom: &str) {
        let new = self.commit(vec![], [atom]);
        self.move_branch(branch, &new);
    }

    fn record_push(&mut self, branch: &str, head: &Sha) {
        self.irreversible_ops += 1;
        self.move_branch(branch, head);
    }

    /// Moves a branch ref (and the tracking PR ref of a still-open PR).
    fn move_branch(&mut self, branch: &str, head: &Sha) {
        self.branches.insert(branch.to_string(), head.clone());
        if let Some(pr) = self.pr_with_branch(branch)
            && matches!(self.prs[&pr].state, ModelPrState::Open)
        {
            self.pr_refs.insert(pr, head.clone());
        }
    }
}

// ─── Stack construction ───

/// A generated stack shape: PR `i+2`'s predecessor is
/// `predecessors[i] + 1 ∈ 1..=i+1` (PR 1 is always the sole root).
/// `advanced[i]` pushes an extra commit to PR `i+1`'s branch *after* its
/// descendants fork, so preparation for those descendants is a real merge
/// (not already-up-to-date) — the push-bracketed path.
#[derive(Debug, Clone)]
struct StackShape {
    predecessors: Vec<usize>,
    advanced: Vec<bool>,
}

fn arb_stack(max_extra: usize) -> impl Strategy<Value = StackShape> {
    (
        prop::collection::vec(any::<prop::sample::Index>(), 0..=max_extra),
        prop::collection::vec(any::<bool>(), max_extra + 1),
    )
        .prop_map(|(choices, mut advanced)| {
            let predecessors: Vec<usize> = choices
                .iter()
                .enumerate()
                .map(|(i, idx)| idx.index(i + 1))
                .collect();
            advanced.truncate(predecessors.len() + 1);
            StackShape {
                predecessors,
                advanced,
            }
        })
}

fn pr_number(index: usize) -> PrNumber {
    PrNumber(index as u64 + 1)
}

fn branch_name(index: usize) -> String {
    format!("pr-{}", index + 1)
}

/// Builds the model world and the seeded `RepoState` for a stack shape.
fn seed(shape: &StackShape) -> (ModelWorld, RepoState) {
    let mut world = ModelWorld::new();
    let n = shape.predecessors.len() + 1;

    let mut prs = HashMap::new();
    for i in 0..n {
        let number = pr_number(i);
        let (base_branch, base_head, predecessor) = if i == 0 {
            ("main".to_string(), world.head("main").unwrap(), None)
        } else {
            let p = shape.predecessors[i - 1];
            (
                branch_name(p),
                world.head(&branch_name(p)).unwrap(),
                Some(pr_number(p)),
            )
        };
        let head = world.commit(vec![base_head], [format!("pr-{}", i + 1)]);
        world.branches.insert(branch_name(i), head.clone());
        world.pr_refs.insert(number, head.clone());
        world.prs.insert(
            number,
            ModelPr {
                branch: branch_name(i),
                base_ref: base_branch.clone(),
                state: ModelPrState::Open,
            },
        );
        prs.insert(
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

    // Advance the chosen branches now that every descendant has forked:
    // their descendants no longer contain the head, so preparation is a
    // real merge followed by a bracketed push.
    for (i, advanced) in shape.advanced.iter().enumerate() {
        if *advanced {
            world.foreign_push(&branch_name(i), &format!("pr-{}-fix", i + 1));
        }
    }

    let snapshot = PersistedRepoSnapshot {
        schema_version: SCHEMA_VERSION,
        snapshot_at: test_timestamp(),
        log_generation: 0,
        log_position: 0,
        next_seq: 0,
        default_branch: "main".to_string(),
        prs,
        active_trains: HashMap::new(),
        seen_dedupe_keys: HashMap::new(),
    };
    (world, RepoState::from_snapshot(snapshot))
}

// ─── The driver: the executor loop M5 will run ───

/// What a train's plan loop ended with.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RunOutcome {
    Done,
    Parked,
    FanOut(Vec<PrNumber>),
}

struct Driver {
    state: RepoState,
    world: ModelWorld,
    log: Vec<StateEvent>,
    seq: u64,
    now: DateTime<Utc>,
    /// (event-count, world) snapshots at every crash point (after each append
    /// batch and after each effect).
    crash_points: Vec<(usize, ModelWorld)>,
    /// Per-train frozen set for the property-3 check.
    frozen: HashMap<PrNumber, Vec<PrNumber>>,
    /// Per-train previous phase for the property-1 check.
    prev_phase: HashMap<PrNumber, CascadePhase>,
    steps: u32,
}

impl Driver {
    fn new(world: ModelWorld, state: RepoState) -> Self {
        Driver {
            state,
            world,
            log: Vec::new(),
            seq: 0,
            now: test_timestamp(),
            crash_points: Vec::new(),
            frozen: HashMap::new(),
            prev_phase: HashMap::new(),
            steps: 0,
        }
    }

    fn append(&mut self, payloads: &[StateEventPayload]) {
        for payload in payloads {
            self.check_event_invariants(payload);
            let event = StateEvent {
                seq: self.seq,
                ts: self.now,
                payload: payload.clone(),
            };
            self.seq += 1;
            self.state.apply_event(&event);
            self.log.push(event);
        }
        self.crash_points.push((self.log.len(), self.world.clone()));
    }

    /// Property 1 (phase legality) and property 3 (frozen-set stability),
    /// checked as the events stream past.
    fn check_event_invariants(&mut self, payload: &StateEventPayload) {
        let StateEventPayload::PhaseTransition {
            train_root, phase, ..
        } = payload
        else {
            return;
        };

        if let Some(prev) = self.prev_phase.get(train_root) {
            verify_transition_invariants(prev, phase)
                .unwrap_or_else(|e| panic!("transition invariant violated for {train_root}: {e}"));
            if prev.kind() != phase.kind() {
                assert!(
                    prev.can_transition_to(phase),
                    "illegal phase transition for {train_root}: {} -> {}",
                    prev.name(),
                    phase.name()
                );
            }
        }
        self.prev_phase.insert(*train_root, phase.clone());

        // Frozen-set checks.
        match phase.kind() {
            PhaseKind::Idle => {
                self.frozen.remove(train_root);
            }
            _ => {
                let frozen = phase
                    .progress()
                    .expect("active phases carry progress")
                    .frozen_descendants()
                    .to_vec();
                match self.frozen.get(train_root) {
                    Some(existing) => assert_eq!(
                        existing, &frozen,
                        "frozen set changed mid-step for {train_root}"
                    ),
                    None => {
                        // Freeze point: must equal the sorted open direct
                        // descendants of the current PR right now.
                        let train = &self.state.active_trains[train_root];
                        let mut expected = crate::state::descendants::collect_direct_descendants(
                            train.current_pr,
                            &self.state.descendants,
                            &self.state.prs,
                        );
                        expected.sort();
                        assert_eq!(
                            frozen, expected,
                            "freeze for {train_root} does not match open direct descendants"
                        );
                        self.frozen.insert(*train_root, frozen);
                    }
                }
            }
        }
    }

    /// Property 2: every irreversible operation is preceded by its durable,
    /// unmatched intent.
    fn check_bracketing(&self, effect: &Effect, root: PrNumber) {
        let facts = ReplayFacts::for_train(&self.log, root);
        match effect {
            Effect::Git(GitEffect::Push { refspec, .. }) => {
                let branch = refspec.strip_prefix("HEAD:refs/heads/").unwrap_or(refspec);
                assert!(
                    facts.unmatched().any(|f| matches!(
                        f,
                        super::IntentFact::PushPrep { branch: b, .. }
                        | super::IntentFact::PushReconcile { branch: b, .. }
                        | super::IntentFact::PushCatchup { branch: b, .. }
                            if b == branch
                    )),
                    "push to {branch} without an unmatched intent"
                );
            }
            Effect::GitHub(GitHubEffect::SquashMerge { pr, .. }) => {
                assert!(
                    facts
                        .unmatched()
                        .any(|f| matches!(f, super::IntentFact::Squash { pr: p, .. } if p == pr)),
                    "squash of {pr} without an unmatched intent"
                );
            }
            Effect::GitHub(GitHubEffect::RetargetPr { pr, .. }) => {
                assert!(
                    facts
                        .unmatched()
                        .any(|f| matches!(f, super::IntentFact::Retarget { pr: p, .. } if p == pr)),
                    "retarget of {pr} without an unmatched intent"
                );
            }
            _ => {}
        }
    }

    /// Runs one plan loop for `root` until it parks, finishes, or fans out.
    /// `between_plans` is called at every observation boundary (the stop
    /// command's injection point).
    fn run_plan(
        &mut self,
        mut plan: StepPlan,
        root: PrNumber,
        between_plans: &mut dyn FnMut(&mut Driver, u32),
    ) -> RunOutcome {
        loop {
            self.steps += 1;
            assert!(self.steps < 2000, "cascade did not terminate");

            let events = plan.events.clone();
            self.append(&events);

            let mut outcomes = Vec::new();
            for effect in &plan.effects {
                self.check_bracketing(effect, root);
                let result = self.world.execute(effect);
                let failed = result.is_err();
                outcomes.push(EffectOutcome {
                    effect: effect.clone(),
                    result,
                });
                self.crash_points.push((self.log.len(), self.world.clone()));
                if failed {
                    break;
                }
            }
            for effect in &plan.best_effort {
                let _ = self.world.execute(effect);
            }

            match plan.control {
                Control::Done => return RunOutcome::Done,
                Control::Park => return RunOutcome::Parked,
                Control::FanOut { new_roots } => return RunOutcome::FanOut(new_roots),
                Control::Continue => {
                    let obs = observe(&outcomes).expect("plan outcomes must be observable");
                    between_plans(self, self.steps);
                    plan = advance(&self.state, root, obs, self.now)
                        .expect("advance must accept its own observations");
                }
            }
        }
    }

    /// Starts the root's train and drives every resulting train (including
    /// fan-out children and re-evaluations after parking) to completion.
    fn run_to_completion(&mut self, root: PrNumber) {
        let plan = start_train(&self.state, root, self.now).expect("start plans");
        self.drive(vec![(root, Some(plan))]);
    }

    /// Drives a queue of `(root, plan)` work items until every train is
    /// finished (a `None` plan means "evaluate from the durable log").
    fn drive(&mut self, mut queue: Vec<(PrNumber, Option<StepPlan>)>) {
        let mut rounds = 0;
        while let Some((r, plan)) = queue.pop() {
            rounds += 1;
            assert!(rounds < 300, "train queue did not drain");
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
            match self.run_plan(plan, r, &mut |_, _| {}) {
                RunOutcome::Done => {}
                // Nothing in the happy path parks forever: re-evaluate.
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

// ─── Final-state assertions shared by the properties ───

/// Every PR merged exactly once; every descendant reconciled against its
/// predecessor's actual squash and retargeted; every train gone; all content
/// on main; no dangling intents.
fn assert_stack_fully_merged(driver: &Driver, shape: &StackShape) {
    let n = shape.predecessors.len() + 1;
    for i in 0..n {
        let number = pr_number(i);
        assert!(
            matches!(driver.world.prs[&number].state, ModelPrState::Merged { .. }),
            "PR {number} did not merge"
        );
        assert_eq!(
            driver.world.squash_count.get(&number),
            Some(&1),
            "PR {number} must be squashed exactly once"
        );
        assert!(driver.state.prs[&number].state.is_merged());
    }
    assert!(
        driver.state.active_trains.is_empty(),
        "all trains must be gone, got {:?}",
        driver.state.active_trains.keys().collect::<Vec<_>>()
    );

    // Property 6: exactly one reconciliation marker per descendant, equal to
    // the predecessor's squash sha (is_root-by-construction feeds off this).
    for i in 1..n {
        let number = pr_number(i);
        let pred = pr_number(shape.predecessors[i - 1]);
        let ModelPrState::Merged { sha: pred_squash } = &driver.world.prs[&pred].state else {
            panic!("predecessor {pred} not merged");
        };
        let markers: Vec<_> = driver
            .log
            .iter()
            .filter_map(|e| match &e.payload {
                StateEventPayload::ReconciliationRecorded { pr, squash_sha } if *pr == number => {
                    Some(squash_sha.clone())
                }
                _ => None,
            })
            .collect();
        assert!(
            !markers.is_empty(),
            "PR {number} must be reconciled at least once"
        );
        assert!(
            markers.iter().all(|m| m == pred_squash),
            "PR {number} reconciled against the wrong squash"
        );
        // The marker must survive to the end of the run: the cascade's own
        // pushes (and their head syncs) must not wipe it.
        assert_eq!(
            driver.state.prs[&number]
                .predecessor_squash_reconciled
                .as_ref(),
            Some(pred_squash),
            "PR {number}'s reconciliation marker was lost"
        );
        assert_eq!(driver.world.prs[&number].base_ref, "main");
    }

    // All content made it to main — including the post-fork fixes, whose
    // whole journey runs through real preparation merges.
    let main_atoms = driver
        .world
        .atoms(&driver.world.head("main").unwrap())
        .clone();
    for i in 0..n {
        assert!(
            main_atoms.contains(&format!("pr-{}", i + 1)),
            "pr-{} content missing from main",
            i + 1
        );
        if shape.advanced.get(i) == Some(&true) {
            assert!(
                main_atoms.contains(&format!("pr-{}-fix", i + 1)),
                "pr-{}'s post-fork fix missing from main",
                i + 1
            );
        }
    }

    // No dangling intents on any train's ledger.
    for i in 0..n {
        let facts = ReplayFacts::for_train(&driver.log, pr_number(i));
        assert_eq!(
            facts.unmatched().count(),
            0,
            "train {} finished with unmatched intents",
            pr_number(i)
        );
    }
}

// ─── Properties ───

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Properties 1, 2, 3, 6, 7 on the happy path: arbitrary stack shapes
    /// (chains and fan-outs) drive to full completion with legal phases,
    /// bracketed irreversible ops, stable frozen sets, exactly-once squashes
    /// and reconciliations, and correct fan-out roots.
    #[test]
    fn cascade_completes_arbitrary_stacks(shape in arb_stack(5)) {
        let (world, state) = seed(&shape);
        let mut driver = Driver::new(world, state);
        driver.run_to_completion(pr_number(0));
        assert_stack_fully_merged(&driver, &shape);

        for event in &driver.log {
            if let StateEventPayload::FanOutCompleted { new_roots, .. } = &event.payload {
                prop_assert!(new_roots.len() > 1, "fan-out with fewer than 2 roots");
            }
        }
    }

    /// Property 4 (the big one): crash anywhere — after any append batch or
    /// any single effect — rebuild the state purely from the durable log,
    /// resume via Evaluate, and the final outcome is the same: every PR
    /// squashed exactly once, all content on main, nothing lost.
    #[test]
    fn crash_resume_equivalence(shape in arb_stack(3)) {
        let (world, state) = seed(&shape);

        // The uninterrupted run (also records every crash point).
        let mut uninterrupted = Driver::new(world.clone(), state.clone());
        uninterrupted.run_to_completion(pr_number(0));
        assert_stack_fully_merged(&uninterrupted, &shape);

        let crash_points = std::mem::take(&mut uninterrupted.crash_points);
        let events = uninterrupted.log;

        for (cut, world_at_cut) in crash_points {
            // Crash wipes everything but the log and the outside world.
            let mut replayed = state.clone();
            for event in &events[..cut] {
                replayed.apply_event(event);
            }
            let mut driver = Driver::new(world_at_cut, replayed);
            driver.log = events[..cut].to_vec();
            driver.seq = cut as u64;

            let mut roots: Vec<PrNumber> = driver
                .state
                .active_trains
                .values()
                .filter(|t| t.state.is_active())
                .map(|t| t.original_root_pr)
                .collect();
            roots.sort();
            if roots.is_empty() && driver.state.active_trains.is_empty() {
                // The crash predates the train (or beat TrainStarted): the
                // start command redelivers.
                driver.run_to_completion(pr_number(0));
            } else {
                driver.drive(roots.into_iter().map(|r| (r, None)).collect());
            }

            assert_stack_fully_merged(&driver, &shape);
        }
    }

    /// Property 5: a stop injected at any observation boundary lands
    /// `TrainStopped` and freezes the world — no further irreversible
    /// operations happen for that train.
    #[test]
    fn stop_is_safe_at_every_boundary(shape in arb_stack(3), stop_at in 1u32..60) {
        let (world, state) = seed(&shape);
        let mut driver = Driver::new(world, state);
        let root = pr_number(0);
        let plan = start_train(&driver.state, root, driver.now).expect("start plans");

        let mut stopped = false;
        let outcome = driver.run_plan(plan, root, &mut |d, step| {
            if step >= stop_at
                && !stopped
                && d.state
                    .active_trains
                    .get(&root)
                    .is_some_and(|t| t.state.is_active())
            {
                stopped = true;
                let stop = stop_train(&d.state, root, false, d.now).expect("stop plans");
                let events = stop.events.clone();
                d.append(&events);
                for effect in &stop.best_effort {
                    let _ = d.world.execute(effect);
                }
            }
        });

        if stopped {
            // The next `advance` call after the stop returned Done without
            // planning anything further.
            prop_assert_eq!(outcome, RunOutcome::Done);
            let train = &driver.state.active_trains[&root];
            let is_stopped = matches!(train.state, TrainState::Stopped { .. });
            prop_assert!(is_stopped, "train must be stopped");
            let ops_at_stop = driver.world.irreversible_ops;

            // Re-evaluating a stopped train plans nothing and changes nothing.
            let facts = ReplayFacts::for_train(&driver.log, root);
            let plan = advance(
                &driver.state,
                root,
                Observation::Evaluate { facts },
                driver.now,
            )
            .expect("evaluate stopped");
            prop_assert_eq!(plan.control, Control::Done);
            prop_assert!(plan.events.is_empty());
            prop_assert!(plan.effects.is_empty());
            prop_assert_eq!(driver.world.irreversible_ops, ops_at_stop);
        } else {
            // The run finished before the injection point: still a full,
            // legal cascade.
            prop_assert!(matches!(outcome, RunOutcome::Done | RunOutcome::FanOut(_)));
        }
    }
}

// ─── Deterministic scenario tests ───

/// A linear two-PR stack drives end to end: one continuous train, no
/// fan-out, one completion.
#[test]
fn linear_stack_end_to_end() {
    let shape = StackShape {
        predecessors: vec![0],
        advanced: vec![true, false],
    };
    let (world, state) = seed(&shape);
    let mut driver = Driver::new(world, state);
    driver.run_to_completion(pr_number(0));
    assert_stack_fully_merged(&driver, &shape);

    assert!(
        !driver
            .log
            .iter()
            .any(|e| matches!(e.payload, StateEventPayload::FanOutCompleted { .. })),
        "a linear stack must not fan out"
    );
    assert_eq!(
        driver
            .log
            .iter()
            .filter(|e| matches!(e.payload, StateEventPayload::TrainCompleted { .. }))
            .count(),
        1
    );
}

/// A fan-out stack (root with two children) retires the original train and
/// creates one per child; driving those completes everything.
#[test]
fn fan_out_creates_independent_trains() {
    let shape = StackShape {
        predecessors: vec![0, 0],
        advanced: vec![true, false, false],
    };
    let (world, state) = seed(&shape);
    let mut driver = Driver::new(world, state);
    driver.run_to_completion(pr_number(0));
    assert_stack_fully_merged(&driver, &shape);

    let fanouts: Vec<_> = driver
        .log
        .iter()
        .filter_map(|e| match &e.payload {
            StateEventPayload::FanOutCompleted {
                old_root,
                new_roots,
                ..
            } => Some((*old_root, new_roots.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(fanouts.len(), 1);
    assert_eq!(fanouts[0].0, pr_number(0));
    assert_eq!(fanouts[0].1, vec![pr_number(1), pr_number(2)]);
}

/// A preparation conflict aborts the train before anything irreversible
/// happens, with a diagnostic comment.
#[test]
fn preparation_conflict_aborts_before_any_irreversible_op() {
    let shape = StackShape {
        predecessors: vec![0],
        advanced: vec![true, false],
    };
    let (mut world, state) = seed(&shape);
    world.conflict_armed = Some(vec!["src/lib.rs".to_string()]);
    let mut driver = Driver::new(world, state);

    let root = pr_number(0);
    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |_, _| {});
    assert_eq!(outcome, RunOutcome::Done);

    let train = &driver.state.active_trains[&root];
    match &train.state {
        TrainState::Aborted { error, .. } => {
            assert_eq!(error.kind, TrainErrorKind::MergeConflict)
        }
        other => panic!("expected abort, got {other:?}"),
    }
    assert_eq!(
        driver.world.irreversible_ops, 0,
        "nothing irreversible before the conflict abort"
    );
    assert!(
        driver
            .world
            .comments
            .iter()
            .any(|(pr, body)| *pr == root && body.contains("aborted")),
        "abort must be diagnosed on the PR"
    );
}

/// A force-push to the predecessor between two descendants' preparations
/// trips the pin rule and aborts (never reaching the squash).
#[test]
fn force_push_mid_preparation_aborts() {
    let shape = StackShape {
        predecessors: vec![0, 0],
        advanced: vec![true, false, false],
    };
    let (world, state) = seed(&shape);
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let mut injected = false;
    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |d, _| {
        // Fire once, after the first descendant's prep push completed.
        if !injected {
            let first_done = d.log.iter().any(
                |e| matches!(&e.payload, StateEventPayload::DonePushPrep { branch, .. } if branch == "pr-2"),
            );
            if first_done {
                injected = true;
                d.world.force_push("pr-1", "rewritten");
            }
        }
    });
    assert!(injected, "the injection point must be reached");
    assert_eq!(outcome, RunOutcome::Done);

    let train = &driver.state.active_trains[&root];
    match &train.state {
        TrainState::Aborted { error, .. } => {
            assert_eq!(error.kind, TrainErrorKind::HeadShaChanged)
        }
        other => panic!("expected abort, got {other:?}"),
    }
    assert_eq!(
        driver.world.squash_count.get(&root),
        None,
        "the squash must never run against a force-pushed head"
    );
}

/// A foreign (fast-forward) push to a descendant mid-preparation is retried:
/// the re-prepared merge incorporates the foreign commit and the cascade
/// completes with it intact.
#[test]
fn foreign_push_during_preparation_is_absorbed() {
    let shape = StackShape {
        predecessors: vec![0],
        advanced: vec![true, false],
    };
    let (world, state) = seed(&shape);
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let mut injected = false;
    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |d, _| {
        // Between the descendant's prep merge and its push: land a foreign
        // commit so the push rejects.
        if !injected {
            let intent_written = d.log.iter().any(
                |e| matches!(&e.payload, StateEventPayload::IntentPushPrep { branch, .. } if branch == "pr-2"),
            );
            if intent_written {
                injected = true;
                d.world.foreign_push("pr-2", "human-fix");
            }
        }
    });
    assert!(injected);
    // Drive whatever remains (parking is possible but not expected here).
    if outcome == RunOutcome::Parked {
        driver.drive(vec![(root, None)]);
    }

    assert_stack_fully_merged(&driver, &shape);
    let main_atoms = driver
        .world
        .atoms(&driver.world.head("main").unwrap())
        .clone();
    assert!(
        main_atoms.contains("human-fix"),
        "the foreign commit must survive the cascade"
    );
}

/// A descendant whose branch disappears mid-cascade is skipped with a
/// warning; the rest of the cascade completes.
#[test]
fn deleted_descendant_branch_is_skipped() {
    let shape = StackShape {
        predecessors: vec![0, 0],
        advanced: vec![false, false, false],
    };
    let (mut world, state) = seed(&shape);
    // Delete pr-2's branch before anything starts.
    world.branches.remove("pr-2");
    let mut driver = Driver::new(world, state);
    driver.run_to_completion(pr_number(0));

    // pr-2 was skipped: never squashed, never reconciled.
    assert_eq!(driver.world.squash_count.get(&pr_number(1)), None);
    assert!(
        driver.log.iter().any(|e| matches!(
            &e.payload,
            StateEventPayload::DescendantSkipped { descendant_pr, .. }
                if *descendant_pr == pr_number(1)
        )),
        "pr-2 must be recorded as skipped"
    );
    assert!(
        !driver.log.iter().any(|e| matches!(
            &e.payload,
            StateEventPayload::ReconciliationRecorded { pr, .. } if *pr == pr_number(1)
        )),
        "a skipped descendant must not be reconciled"
    );
    // pr-3 continues the train (a fan-out of one) and merges.
    assert_eq!(driver.world.squash_count.get(&pr_number(2)), Some(&1));
    assert_eq!(driver.world.squash_count.get(&pr_number(0)), Some(&1));
    assert!(driver.state.active_trains.is_empty());
}

/// A descendant closed mid-cascade (after reconciliation, before retarget)
/// is skipped when the retarget fails with "not open"; the cascade completes
/// with the surviving sibling.
#[test]
fn descendant_closed_mid_cascade_is_skipped() {
    let shape = StackShape {
        predecessors: vec![0, 0],
        advanced: vec![true, false, false],
    };
    let (world, state) = seed(&shape);
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let mut injected = false;
    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |d, _| {
        // Once pr-2's reconcile push is durable, a human closes pr-2. The
        // webhook lands as a PrClosed event; the world stops accepting
        // mutations for it.
        if !injected {
            let reconciled = d.log.iter().any(|e| {
                matches!(&e.payload, StateEventPayload::DonePushReconcile { branch, .. } if branch == "pr-2")
            });
            if reconciled {
                injected = true;
                d.world.prs.get_mut(&pr_number(1)).unwrap().state = ModelPrState::Closed;
                let events = [StateEventPayload::PrClosed { pr: pr_number(1) }];
                d.append(&events);
            }
        }
    });
    assert!(injected, "the injection point must be reached");

    // pr-2 was skipped at whichever operation first hit the closed PR; the
    // sibling pr-3 continues the cascade to completion.
    if outcome == RunOutcome::Parked {
        driver.drive(vec![(root, None)]);
    } else if let RunOutcome::FanOut(roots) = outcome {
        driver.drive(roots.into_iter().map(|r| (r, None)).collect());
    }
    assert!(
        driver.log.iter().any(|e| matches!(
            &e.payload,
            StateEventPayload::DescendantSkipped { descendant_pr, .. }
                if *descendant_pr == pr_number(1)
        )),
        "pr-2 must be recorded as skipped"
    );
    assert_eq!(driver.world.squash_count.get(&pr_number(1)), None);
    assert_eq!(driver.world.squash_count.get(&pr_number(0)), Some(&1));
    assert_eq!(driver.world.squash_count.get(&pr_number(2)), Some(&1));
    assert!(driver.state.active_trains.is_empty());
}

/// A current PR retargeted away from the default branch (while parked or
/// between effects) must never be squashed — GitHub merges into the PR's
/// current base, so proceeding would merge the stack into the wrong branch.
#[test]
fn retargeted_current_pr_aborts_instead_of_squashing() {
    let shape = StackShape {
        predecessors: vec![],
        advanced: vec![false],
    };
    let (mut world, state) = seed(&shape);
    // A human retargets the PR before the train evaluates; the cache still
    // says `main`.
    world.prs.get_mut(&pr_number(0)).unwrap().base_ref = "release".to_string();
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |_, _| {});
    assert_eq!(outcome, RunOutcome::Done);

    match &driver.state.active_trains[&root].state {
        TrainState::Aborted { error, .. } => {
            assert_eq!(error.kind, TrainErrorKind::BaseBranchMismatch)
        }
        other => panic!("expected abort, got {other:?}"),
    }
    assert_eq!(
        driver.world.squash_count.get(&root),
        None,
        "a retargeted PR must never be squashed"
    );
    // The refetched base was persisted, not just acted on.
    assert_eq!(driver.state.prs[&root].base_ref, "release");
}

/// When the refetch is the only witness of an external merge, the fetched
/// state must land in the cache: the train completes AND the cached PR is
/// merged, so future starts/topology don't operate on a phantom-open PR.
#[test]
fn externally_merged_current_pr_completes_and_updates_cache() {
    let shape = StackShape {
        predecessors: vec![],
        advanced: vec![false],
    };
    let (mut world, state) = seed(&shape);
    world.external_squash(pr_number(0));
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |_, _| {});
    assert_eq!(outcome, RunOutcome::Done);

    assert!(
        driver.state.active_trains.is_empty(),
        "an externally merged PR with no descendants completes the train"
    );
    assert!(
        driver.state.prs[&root].state.is_merged(),
        "the externally observed merge must be persisted to the cache"
    );
    assert_eq!(
        driver.world.squash_count.get(&root),
        None,
        "the bot must not re-squash an externally merged PR"
    );
    // And a fresh `start` on it is now correctly rejected as not-open.
    let plan = start_train(&driver.state, root, driver.now).unwrap();
    assert_eq!(plan.control, Control::Done);
    assert!(plan.events.is_empty());
}

/// `stop` on an unfrozen descendant below the current PR must find the train
/// even after intermediate members merged: the open-only walk from the
/// original root is blocked by the merged members, so membership must also
/// traverse from the current PR.
#[test]
fn stop_below_merged_members_still_stops_the_train() {
    // 1 ← 2 ← 3 ← 4; the train will merge 1 and 2, then park at current=3.
    let shape = StackShape {
        predecessors: vec![0, 1, 2],
        advanced: vec![false; 4],
    };
    let (mut world, state) = seed(&shape);
    world
        .merge_states
        .insert(pr_number(2), MergeStateStatus::Blocked);
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |_, _| {});
    assert_eq!(outcome, RunOutcome::Parked);
    let train = &driver.state.active_trains[&root];
    assert_eq!(train.current_pr, pr_number(2), "train parked at PR 3");
    assert!(driver.state.prs[&pr_number(0)].state.is_merged());
    assert!(driver.state.prs[&pr_number(1)].state.is_merged());

    // Stop issued on PR 4 — an unfrozen descendant below the parked head.
    let stop = stop_train(&driver.state, pr_number(3), false, driver.now).unwrap();
    assert!(
        stop.events
            .iter()
            .any(|e| matches!(e, StateEventPayload::TrainStopped { root_pr } if *root_pr == root)),
        "stop below merged members must stop the active train, got {stop:?}"
    );
    let events = stop.events.clone();
    driver.append(&events);
    assert!(matches!(
        driver.state.active_trains[&root].state,
        TrainState::Stopped { .. }
    ));
}

/// A descendant that closed after its retarget landed (but before the done
/// event) is skipped during recovery, not completed: completing it would make
/// a closed PR the train's next head and abort the whole train one refetch
/// later.
#[test]
fn descendant_closed_after_retarget_is_skipped_on_recovery() {
    let shape = StackShape {
        predecessors: vec![0],
        advanced: vec![true, false],
    };
    let (world, state) = seed(&shape);

    // Uninterrupted run to harvest the crash point where pr-2's retarget
    // landed but its done-event did not.
    let mut uninterrupted = Driver::new(world, state.clone());
    uninterrupted.run_to_completion(pr_number(0));
    let crash_points = std::mem::take(&mut uninterrupted.crash_points);
    let events = uninterrupted.log;

    let (cut, mut world_at_cut) = crash_points
        .into_iter()
        .find(|(cut, world)| {
            let facts = ReplayFacts::for_train(&events[..*cut], pr_number(0));
            let unmatched_retarget = facts.unmatched().any(
                |f| matches!(f, super::IntentFact::Retarget { pr, .. } if *pr == pr_number(1)),
            );
            unmatched_retarget && world.prs[&pr_number(1)].base_ref == "main"
        })
        .expect("a crash point between the retarget and its done event exists");

    // The human closes pr-2 in the crash window.
    world_at_cut.prs.get_mut(&pr_number(1)).unwrap().state = ModelPrState::Closed;

    let mut replayed = state.clone();
    for event in &events[..cut] {
        replayed.apply_event(event);
    }
    let mut driver = Driver::new(world_at_cut, replayed);
    driver.log = events[..cut].to_vec();
    driver.seq = cut as u64;
    driver.drive(vec![(pr_number(0), None)]);

    assert!(
        driver.log[cut..].iter().any(|e| matches!(
            &e.payload,
            StateEventPayload::DescendantSkipped { descendant_pr, .. }
                if *descendant_pr == pr_number(1)
        )),
        "the closed descendant must be skipped"
    );
    assert!(
        !driver
            .log
            .iter()
            .any(|e| matches!(e.payload, StateEventPayload::TrainAborted { .. })),
        "the train must not abort because a descendant closed"
    );
    assert!(
        driver.state.active_trains.is_empty(),
        "with its only descendant skipped, the train completes"
    );
    assert_eq!(
        driver.world.squash_count.get(&pr_number(1)),
        None,
        "a closed descendant must never become the train head and be squashed"
    );
}

/// BEHIND at evaluation time: the root is updated with main (intent-bracketed
/// push), the train parks for CI, and a later evaluation completes the run.
#[test]
fn behind_root_is_updated_then_train_completes() {
    let shape = StackShape {
        predecessors: vec![],
        advanced: vec![false],
    };
    let (mut world, state) = seed(&shape);
    // Someone landed a commit on main; strict protection reports BEHIND.
    world.foreign_push("main", "landed-on-main");
    world
        .merge_states
        .insert(pr_number(0), MergeStateStatus::Behind);
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |_, _| {});
    // The behind-fix pushed and parked for CI.
    assert_eq!(outcome, RunOutcome::Parked);
    assert_eq!(
        driver.state.active_trains[&root].state,
        TrainState::WaitingCi
    );
    assert!(driver.log.iter().any(
        |e| matches!(&e.payload, StateEventPayload::DonePushPrep { branch, .. } if branch == "pr-1")
    ));

    // CI passes on the new head.
    driver
        .world
        .merge_states
        .insert(root, MergeStateStatus::Clean);
    driver.drive(vec![(root, None)]);
    assert_stack_fully_merged(&driver, &shape);

    let main_atoms = driver
        .world
        .atoms(&driver.world.head("main").unwrap())
        .clone();
    assert!(main_atoms.contains("landed-on-main"));
}

/// Start-command validation: rejections are comments, not trains.
#[test]
fn start_rejections_are_comments() {
    let shape = StackShape {
        predecessors: vec![0],
        advanced: vec![false, false],
    };
    let (world, state) = seed(&shape);
    let mut driver = Driver::new(world, state);

    // Starting on a non-root (the descendant) is rejected.
    let plan = start_train(&driver.state, pr_number(1), driver.now).unwrap();
    assert_eq!(plan.control, Control::Done);
    assert!(plan.events.is_empty());
    assert!(matches!(
        plan.best_effort.as_slice(),
        [Effect::GitHub(GitHubEffect::PostComment { .. })]
    ));

    // A running train rejects a second start anywhere in the stack.
    driver.state.apply_event(&StateEvent {
        seq: 0,
        ts: driver.now,
        payload: StateEventPayload::TrainStarted {
            root_pr: pr_number(0),
            current_pr: pr_number(0),
        },
    });
    for pr in [pr_number(0), pr_number(1)] {
        let plan = start_train(&driver.state, pr, driver.now).unwrap();
        assert_eq!(
            plan.control,
            Control::Done,
            "start on {pr} must be rejected"
        );
        assert!(plan.events.is_empty());
    }
}

/// The merge-method preflight failure refuses to create a train.
#[test]
fn preflight_failure_refuses_to_start() {
    let shape = StackShape {
        predecessors: vec![],
        advanced: vec![false],
    };
    let (mut world, state) = seed(&shape);
    world.settings.allow_merge_commit = true;
    let mut driver = Driver::new(world, state);
    let root = pr_number(0);

    let plan = start_train(&driver.state, root, driver.now).unwrap();
    let outcome = driver.run_plan(plan, root, &mut |_, _| {});
    assert_eq!(outcome, RunOutcome::Done);
    assert!(
        driver.state.active_trains.is_empty(),
        "no train may exist after a failed preflight"
    );
    assert!(
        !driver
            .log
            .iter()
            .any(|e| matches!(e.payload, StateEventPayload::TrainStarted { .. })),
        "no TrainStarted event may be appended"
    );
    assert!(driver.world.comments.iter().any(|(pr, _)| *pr == root));
}

/// Stop with no train involved is a courtesy comment.
#[test]
fn stop_without_train_is_a_comment() {
    let shape = StackShape {
        predecessors: vec![],
        advanced: vec![false],
    };
    let (world, state) = seed(&shape);
    let driver = Driver::new(world, state);
    let plan = stop_train(&driver.state, pr_number(0), false, driver.now).unwrap();
    assert_eq!(plan.control, Control::Done);
    assert!(plan.events.is_empty());
    assert!(matches!(
        plan.best_effort.as_slice(),
        [Effect::GitHub(GitHubEffect::PostComment { .. })]
    ));
}
