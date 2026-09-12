//! A fake GitHub backed by a *real* git remote, for engine/worker tests.
//!
//! [`FakeGitHub`] fakes only the API half of the world: squash-merges create
//! real squash commits on the bare remote (via
//! `git::test_support::squash_merge_to_main`), `RefetchPr`/`GetPr` read real
//! refs, and `refs/pull/<n>/head` is re-mirrored by [`FakeGitHub::sync_pr_refs`]
//! after effects, exactly as GitHub tracks PR branches while open and freezes
//! them at merge. Everything the engine observes through GitHub therefore
//! stays consistent with what the real git interpreter does to the repo.
//!
//! Extracted from `cascade::conformance_tests` so the worker's integration
//! tests (M5) can drive the same world through the executor seam.

use std::collections::HashMap;

use crate::cascade::EffectError;
use crate::effects::github::{CollaboratorRole, GitHubEffect};
use crate::effects::{GitHubResponse, PrData, RepoSettingsData};
use crate::git::test_support::{create_pr_ref, squash_merge_to_main};
use crate::git::{GitConfig, run_git_stdout};
use crate::types::{CommentId, MergeStateStatus, PrNumber, PrState, Sha, TrainErrorKind};

/// A fake PR's lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FakePrState {
    Open,
    /// Closed without merging.
    Closed,
    Merged {
        squash_sha: Sha,
    },
}

/// A PR as the fake GitHub tracks it.
#[derive(Debug, Clone)]
pub struct FakePr {
    pub branch: String,
    pub base_ref: String,
    pub state: FakePrState,
    /// The PR author's user id (author-gated decisions read it).
    pub author_id: u64,
}

/// A comment as the fake GitHub stores it (the live, mutable copy that
/// `UpdateComment` edits and `ListComments` returns — unlike
/// `posted_comments`, which is an append-only log of `PostComment` calls).
#[derive(Debug, Clone)]
pub struct FakeComment {
    pub pr: PrNumber,
    pub author_id: u64,
    pub body: String,
    /// Set by `UpdateComment` (and seedable): mirrors GitHub's
    /// `updated_at > created_at`.
    pub edited: bool,
}

/// The GitHub half of a test world whose git half is real.
pub struct FakeGitHub {
    pub config: GitConfig,
    pub prs: HashMap<PrNumber, FakePr>,
    pub next_comment: u64,
    /// Squash acceptances per PR — the ≤1-squash oracle reads this.
    pub squash_count: HashMap<PrNumber, u32>,
    /// Collaborator roles by username, for `GetCollaboratorPermission`.
    /// Unlisted users answer `CollaboratorRole::None`.
    pub roles: HashMap<String, CollaboratorRole>,
    /// Every `PostComment` body, for asserting rejections/acks.
    pub posted_comments: Vec<(PrNumber, String)>,
    /// Live comments by id (`BTreeMap` so `ListComments` is id-ordered).
    /// `PostComment` inserts with `comment_author` as the author;
    /// `UpdateComment` edits in place (404 if deleted); tests may insert
    /// user-authored comments or delete the bot's to exercise recovery.
    pub comments: std::collections::BTreeMap<CommentId, FakeComment>,
    /// The author id stamped on bot-posted comments (the worker tests set
    /// this to the bot's user id so status comments pass recovery's
    /// author check).
    pub comment_author: u64,
    /// Outage injection: while set, every effect fails `Transient`.
    pub unavailable: bool,
    /// While set, `GetCollaboratorPermission` fails `Permanent` (e.g. the
    /// token lacks the scope for the collaborators API).
    pub permission_lookup_broken: bool,
    /// `GetRepoSettings` attempts (including failed ones), for asserting the
    /// worker's stall-retry behaviour.
    pub settings_fetches: u32,
    /// Open PRs that report `Blocked` mergeability instead of `Clean` — a
    /// frontier PR here parks its train `WaitingCi`, so tests can exercise
    /// the missed-webhook polling fallback (clear the set, then poll).
    pub blocked: std::collections::HashSet<PrNumber>,
    /// Comments that EXIST but are omitted from `ListComments`: GitHub is
    /// not read-after-write consistent, so a freshly posted comment can be
    /// missing from a listing that a moment later returns it. Recovery
    /// paths that read absence as deletion must survive this.
    pub hidden_from_listings: std::collections::HashSet<CommentId>,
    /// `UpdateComment` calls that reached a live comment, so a test can
    /// assert that a satisfied obligation writes nothing further.
    pub comment_updates: u32,
    /// Comments `ListComments` still serves although they no longer
    /// exist: GitHub's listing cache can keep returning a deleted
    /// comment for a while. Writes to them 404, exactly like GitHub.
    pub stale_listing_ghosts: std::collections::BTreeMap<CommentId, FakeComment>,
    /// Bodies `ListComments` serves INSTEAD of the stored ones, per
    /// comment id: GitHub's listing cache can lag an edit, returning a
    /// pre-edit body for a comment whose true content has moved on.
    /// `UpdateComment` and direct reads see the real comment.
    pub stale_listing_bodies: std::collections::BTreeMap<CommentId, String>,
    /// While set, `PostComment` CREATES the comment but reports a
    /// transient failure — the response is lost on the wire. The comment
    /// exists; nothing acknowledged it.
    pub post_comment_response_lost: bool,
    /// While set, `UpdateComment` fails `Permanent` while everything else
    /// keeps working: the token can still READ comments but has lost the
    /// right to edit them, so an owed rewrite can never land.
    pub update_comment_broken: bool,
}

impl FakeGitHub {
    pub fn new(config: GitConfig, prs: HashMap<PrNumber, FakePr>) -> FakeGitHub {
        FakeGitHub {
            config,
            prs,
            next_comment: 1,
            squash_count: HashMap::new(),
            roles: HashMap::new(),
            posted_comments: Vec::new(),
            comments: std::collections::BTreeMap::new(),
            comment_author: 0,
            unavailable: false,
            permission_lookup_broken: false,
            settings_fetches: 0,
            blocked: std::collections::HashSet::new(),
            hidden_from_listings: std::collections::HashSet::new(),
            comment_updates: 0,
            stale_listing_ghosts: std::collections::BTreeMap::new(),
            stale_listing_bodies: std::collections::BTreeMap::new(),
            post_comment_response_lost: false,
            update_comment_broken: false,
        }
    }

    /// The real head of a PR's branch on the bare remote.
    pub fn branch_head(&self, branch: &str) -> Sha {
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
    pub fn sync_pr_refs(&self) {
        for (number, pr) in &self.prs {
            if matches!(pr.state, FakePrState::Open) {
                let head = self.branch_head(&pr.branch);
                create_pr_ref(&self.config, number.0, &head);
            }
        }
    }

    /// The `PrData` GitHub would return for `pr` right now.
    fn pr_data(&self, pr: PrNumber) -> (PrData, MergeStateStatus) {
        let fake = self.prs.get(&pr).expect("fetch of a known PR");
        let (state, head_sha, merge_state) = match &fake.state {
            FakePrState::Open => (
                PrState::Open,
                self.branch_head(&fake.branch),
                if self.blocked.contains(&pr) {
                    MergeStateStatus::Blocked
                } else {
                    MergeStateStatus::Clean
                },
            ),
            FakePrState::Closed => (
                PrState::Closed,
                self.branch_head(&fake.branch),
                MergeStateStatus::Unknown,
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
        (
            PrData {
                number: pr,
                head_sha,
                head_ref: fake.branch.clone(),
                base_ref: fake.base_ref.clone(),
                state,
                is_draft: false,
                author_id: fake.author_id,
            },
            merge_state,
        )
    }

    pub fn execute(&mut self, effect: &GitHubEffect) -> Result<GitHubResponse, EffectError> {
        if matches!(effect, GitHubEffect::GetRepoSettings) {
            self.settings_fetches += 1;
        }
        if self.unavailable {
            return Err(EffectError::Transient {
                detail: "fake GitHub outage (test-injected)".to_owned(),
            });
        }
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
                // Real GitHub answers 404 for a PR that does not exist —
                // a Permanent error, not a panic.
                if !self.prs.contains_key(pr) {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: format!("no such PR #{pr} (fake 404)"),
                    });
                }
                let (data, merge_state) = self.pr_data(*pr);
                Ok(GitHubResponse::PrRefetched {
                    pr: data,
                    merge_state,
                })
            }

            GitHubEffect::GetPr { pr } => {
                if !self.prs.contains_key(pr) {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: format!("no such PR #{pr} (fake 404)"),
                    });
                }
                Ok(GitHubResponse::Pr(self.pr_data(*pr).0))
            }

            GitHubEffect::GetCollaboratorPermission { username } => {
                if self.permission_lookup_broken {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: "permission lookup broken (test-injected)".to_owned(),
                    });
                }
                Ok(GitHubResponse::CollaboratorPermission {
                    role: self
                        .roles
                        .get(username)
                        .cloned()
                        .unwrap_or(CollaboratorRole::None),
                })
            }

            GitHubEffect::PostComment { pr, body } => {
                // GitHub comment ids are globally monotonic — a new comment
                // always outranks every existing one, including comments
                // tests seeded directly into `comments`. Recovery's
                // extension watermark relies on this ordering.
                let floor = self.comments.keys().next_back().map_or(0, |max| max.0 + 1);
                let id = CommentId(self.next_comment.max(floor));
                self.next_comment = id.0 + 1;
                self.posted_comments.push((*pr, body.clone()));
                self.comments.insert(
                    id,
                    FakeComment {
                        pr: *pr,
                        author_id: self.comment_author,
                        body: body.clone(),
                        edited: false,
                    },
                );
                if self.post_comment_response_lost {
                    return Err(EffectError::Transient {
                        detail: "post landed but the response was lost (test-injected)".to_owned(),
                    });
                }
                Ok(GitHubResponse::CommentPosted { id })
            }
            GitHubEffect::UpdateComment { comment_id, body } => {
                if self.update_comment_broken {
                    return Err(EffectError::Permanent {
                        kind: TrainErrorKind::ApiError,
                        detail: format!("cannot edit comment {comment_id} (fake 403)"),
                    });
                }
                let updates = &mut self.comment_updates;
                match self.comments.get_mut(comment_id) {
                    Some(comment) => {
                        comment.body = body.clone();
                        comment.edited = true;
                        *updates += 1;
                        Ok(GitHubResponse::CommentUpdated)
                    }
                    // A deleted comment 404s, exactly like GitHub.
                    None => Err(EffectError::Permanent {
                        kind: TrainErrorKind::NotFound,
                        detail: format!("no such comment {comment_id} (fake 404)"),
                    }),
                }
            }
            GitHubEffect::ListOpenPrs => {
                let mut numbers: Vec<PrNumber> = self
                    .prs
                    .iter()
                    .filter(|(_, p)| matches!(p.state, FakePrState::Open))
                    .map(|(n, _)| *n)
                    .collect();
                numbers.sort_unstable();
                Ok(GitHubResponse::PrList(
                    numbers.into_iter().map(|n| self.pr_data(n).0).collect(),
                ))
            }
            GitHubEffect::ListRecentlyMergedPrs { .. } => {
                let mut numbers: Vec<PrNumber> = self
                    .prs
                    .iter()
                    .filter(|(_, p)| matches!(p.state, FakePrState::Merged { .. }))
                    .map(|(n, _)| *n)
                    .collect();
                numbers.sort_unstable();
                Ok(GitHubResponse::RecentlyMergedPrList {
                    prs: numbers.into_iter().map(|n| self.pr_data(n).0).collect(),
                    may_be_incomplete: false,
                })
            }
            GitHubEffect::ListComments { pr } => Ok(GitHubResponse::Comments(
                self.comments
                    .iter()
                    .filter(|(id, c)| c.pr == *pr && !self.hidden_from_listings.contains(id))
                    .map(|(id, c)| crate::effects::github::CommentData {
                        id: *id,
                        author_id: c.author_id,
                        body: self
                            .stale_listing_bodies
                            .get(id)
                            .cloned()
                            .unwrap_or_else(|| c.body.clone()),
                        edited: c.edited,
                    })
                    .chain(
                        self.stale_listing_ghosts
                            .iter()
                            .filter(|(_, c)| c.pr == *pr)
                            .map(|(id, c)| crate::effects::github::CommentData {
                                id: *id,
                                author_id: c.author_id,
                                body: c.body.clone(),
                                edited: c.edited,
                            }),
                    )
                    .collect(),
            )),
            GitHubEffect::AddReaction { .. } => Ok(GitHubResponse::ReactionAdded),

            other => panic!("the engine does not emit {other:?}"),
        }
    }
}
