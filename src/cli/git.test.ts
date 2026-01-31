import { describe, it, expect } from "vitest";
import { getCurrentGitBranch, isMainBranch, isGitRepo, sanitizeBranchName, getTinybirdBranchName } from "./git.js";

describe("Git utilities", () => {
  describe("isGitRepo", () => {
    it("returns true in a git repo", () => {
      // This test file is in a git repo
      expect(isGitRepo()).toBe(true);
    });
  });

  describe("getCurrentGitBranch", () => {
    it("returns a string in a git repo", () => {
      const branch = getCurrentGitBranch();
      // We're in a git repo, so it should return a branch name
      expect(branch).not.toBeNull();
      expect(typeof branch).toBe("string");
    });
  });

  describe("isMainBranch", () => {
    it("returns a boolean", () => {
      const result = isMainBranch();
      expect(typeof result).toBe("boolean");
    });
  });

  describe("sanitizeBranchName", () => {
    it("keeps alphanumeric characters", () => {
      expect(sanitizeBranchName("feature123")).toBe("feature123");
    });

    it("keeps underscores", () => {
      expect(sanitizeBranchName("my_feature")).toBe("my_feature");
    });

    it("replaces hyphens with underscores", () => {
      expect(sanitizeBranchName("my-feature")).toBe("my_feature");
    });

    it("replaces slashes with underscores", () => {
      expect(sanitizeBranchName("feature/add-login")).toBe("feature_add_login");
    });

    it("replaces dots with underscores", () => {
      expect(sanitizeBranchName("release.1.0")).toBe("release_1_0");
    });

    it("replaces multiple symbols with single underscore", () => {
      expect(sanitizeBranchName("feature--test")).toBe("feature_test");
      expect(sanitizeBranchName("a///b")).toBe("a_b");
    });

    it("removes leading and trailing underscores", () => {
      expect(sanitizeBranchName("-feature-")).toBe("feature");
      expect(sanitizeBranchName("_feature_")).toBe("feature");
    });

    it("handles complex branch names", () => {
      expect(sanitizeBranchName("feature/JIRA-123/add-user-auth")).toBe("feature_JIRA_123_add_user_auth");
      expect(sanitizeBranchName("user@name/branch")).toBe("user_name_branch");
    });
  });

  describe("getTinybirdBranchName", () => {
    it("returns a sanitized string in a git repo", () => {
      const branch = getTinybirdBranchName();
      // We're in a git repo, so it should return a sanitized branch name
      expect(branch).not.toBeNull();
      expect(typeof branch).toBe("string");
      // Should only contain valid characters
      expect(branch).toMatch(/^[a-zA-Z0-9_]+$/);
    });
  });
});
