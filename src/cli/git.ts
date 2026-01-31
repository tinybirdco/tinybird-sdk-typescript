/**
 * Git utilities for branch detection
 */

import { execSync } from "child_process";

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

    // HEAD means detached HEAD state
    if (branch === "HEAD") {
      return null;
    }

    return branch;
  } catch {
    // Not in a git repo or git not available
    return null;
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
 * Returns null if not in a git repo or on detached HEAD
 */
export function getTinybirdBranchName(): string | null {
  const branch = getCurrentGitBranch();
  if (!branch) return null;
  return sanitizeBranchName(branch);
}
