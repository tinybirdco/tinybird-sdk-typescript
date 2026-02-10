/**
 * Git utilities for branch detection
 */

import { execSync } from "child_process";

/**
 * Get the branch name from CI environment variables
 * Returns null if not in a known CI environment
 */
function getBranchFromCIEnv(): string | null {
  // Vercel
  if (process.env.VERCEL_GIT_COMMIT_REF) {
    return process.env.VERCEL_GIT_COMMIT_REF;
  }

  // GitHub Actions
  // GITHUB_HEAD_REF is set for pull requests, GITHUB_REF_NAME for pushes
  const githubBranch = process.env.GITHUB_HEAD_REF || process.env.GITHUB_REF_NAME;
  if (githubBranch) {
    return githubBranch;
  }

  // GitLab CI
  if (process.env.CI_COMMIT_BRANCH) {
    return process.env.CI_COMMIT_BRANCH;
  }

  // CircleCI
  if (process.env.CIRCLE_BRANCH) {
    return process.env.CIRCLE_BRANCH;
  }

  // Azure Pipelines
  if (process.env.BUILD_SOURCEBRANCHNAME) {
    return process.env.BUILD_SOURCEBRANCHNAME;
  }

  // Bitbucket Pipelines
  if (process.env.BITBUCKET_BRANCH) {
    return process.env.BITBUCKET_BRANCH;
  }

  // Jenkins
  if (process.env.GIT_BRANCH) {
    // Jenkins prefixes with origin/, remove it
    return process.env.GIT_BRANCH.replace(/^origin\//, "");
  }

  // Travis CI
  if (process.env.TRAVIS_BRANCH) {
    return process.env.TRAVIS_BRANCH;
  }

  return null;
}

/**
 * Get the current git branch name
 * Returns null if not in a git repo or on detached HEAD
 */
export function getCurrentGitBranch(): string | null {
  try {
    const branch = execSync("git rev-parse --abbrev-ref HEAD", {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();

    // HEAD means detached HEAD state - try CI environment variables
    if (branch === "HEAD") {
      return getBranchFromCIEnv();
    }

    return branch;
  } catch {
    // Not in a git repo or git not available
    // Still check CI env vars as fallback
    return getBranchFromCIEnv();
  }
}

/**
 * Check if we're on the main/master branch
 */
export function isMainBranch(): boolean {
  const branch = getCurrentGitBranch();
  return branch === "main" || branch === "master";
}

/**
 * Check if we're in a git repository
 */
export function isGitRepo(): boolean {
  try {
    execSync("git rev-parse --git-dir", {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });
    return true;
  } catch {
    return false;
  }
}

/**
 * Get the root directory of the git repository
 * Returns null if not in a git repo
 */
export function getGitRoot(): string | null {
  try {
    return execSync("git rev-parse --show-toplevel", {
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
  } catch {
    return null;
  }
}

/**
 * Sanitize a git branch name for use as a Tinybird branch name
 * Tinybird only accepts alphanumeric characters and underscores
 * All other characters are replaced with underscores
 */
export function sanitizeBranchName(branchName: string): string {
  // Replace any character that is not alphanumeric or underscore with underscore
  // Also collapse multiple consecutive underscores into one
  return branchName
    .replace(/[^a-zA-Z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, ""); // Remove leading/trailing underscores
}

/**
 * Get the current git branch name sanitized for Tinybird
 * Returns null if not in a git repo, on detached HEAD, or if the
 * sanitized name would be empty (e.g., branch name "----")
 */
export function getTinybirdBranchName(): string | null {
  const branch = getCurrentGitBranch();
  if (!branch) return null;
  const sanitized = sanitizeBranchName(branch);
  if (!sanitized) return null;
  return sanitized;
}
