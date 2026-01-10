//! Merge Train Bot - A GitHub bot for orchestrating squash-merge cascades of stacked PRs.
//!
//! This library provides the core domain types and logic for the merge train bot.

#[cfg(test)]
pub mod test_utils;

pub mod cascade;
pub mod commands;
pub mod effects;
pub mod git;
pub mod github;
pub mod persistence;
pub mod spool;
pub mod state;
pub mod status;
pub mod types;
