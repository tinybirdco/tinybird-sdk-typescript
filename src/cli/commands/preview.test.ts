import { describe, it, expect } from "vitest";
import { generatePreviewBranchName } from "./preview.js";

describe("Preview command", () => {
  describe("generatePreviewBranchName", () => {
    it("generates name with tmp_ci prefix", () => {
      const result = generatePreviewBranchName("feature-branch");
      expect(result).toBe("tmp_ci_feature_branch");
    });

    it("sanitizes branch name with slashes", () => {
      const result = generatePreviewBranchName("feature/add-login");
      expect(result).toBe("tmp_ci_feature_add_login");
    });

    it("sanitizes branch name with dots", () => {
      const result = generatePreviewBranchName("release.1.0");
      expect(result).toBe("tmp_ci_release_1_0");
    });

    it("handles complex branch names", () => {
      const result = generatePreviewBranchName("feature/JIRA-123/add-user-auth");
      expect(result).toBe("tmp_ci_feature_JIRA_123_add_user_auth");
    });

    it("uses 'unknown' when branch is null", () => {
      const result = generatePreviewBranchName(null);
      expect(result).toBe("tmp_ci_unknown");
    });

    it("produces deterministic names for same branch", () => {
      const result1 = generatePreviewBranchName("test-branch");
      const result2 = generatePreviewBranchName("test-branch");
      expect(result1).toBe(result2);
    });
  });
});
