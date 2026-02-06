import { describe, it, expect, beforeEach, vi } from "vitest";
import { generatePreviewBranchName } from "./preview.js";

describe("Preview command", () => {
  describe("generatePreviewBranchName", () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date("2024-02-06T12:00:00Z"));
    });

    it("generates name with branch and timestamp", () => {
      const result = generatePreviewBranchName("feature-branch");
      expect(result).toBe("tmp_ci_feature_branch_1707220800");
    });

    it("sanitizes branch name with slashes", () => {
      const result = generatePreviewBranchName("feature/add-login");
      expect(result).toBe("tmp_ci_feature_add_login_1707220800");
    });

    it("sanitizes branch name with dots", () => {
      const result = generatePreviewBranchName("release.1.0");
      expect(result).toBe("tmp_ci_release_1_0_1707220800");
    });

    it("handles complex branch names", () => {
      const result = generatePreviewBranchName("feature/JIRA-123/add-user-auth");
      expect(result).toBe("tmp_ci_feature_JIRA_123_add_user_auth_1707220800");
    });

    it("uses 'unknown' when branch is null", () => {
      const result = generatePreviewBranchName(null);
      expect(result).toBe("tmp_ci_unknown_1707220800");
    });

    it("uses unix timestamp in seconds", () => {
      // 1707220800 is 2024-02-06T12:00:00Z in seconds
      const result = generatePreviewBranchName("test");
      expect(result).toMatch(/_1707220800$/);
    });
  });
});
