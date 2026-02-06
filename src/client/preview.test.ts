import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  isPreviewEnvironment,
  getPreviewBranchName,
  resolveToken,
  clearTokenCache,
} from "./preview.js";

describe("Preview environment detection", () => {
  const originalEnv = { ...process.env };

  beforeEach(() => {
    // Clear all relevant env vars before each test
    delete process.env.VERCEL_ENV;
    delete process.env.GITHUB_HEAD_REF;
    delete process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME;
    delete process.env.CI;
    delete process.env.TINYBIRD_PREVIEW_MODE;
    delete process.env.VERCEL_GIT_COMMIT_REF;
    delete process.env.GITHUB_REF_NAME;
    delete process.env.CI_COMMIT_BRANCH;
    delete process.env.CIRCLE_BRANCH;
    delete process.env.BUILD_SOURCEBRANCHNAME;
    delete process.env.BITBUCKET_BRANCH;
    delete process.env.TINYBIRD_BRANCH_NAME;
    delete process.env.TINYBIRD_BRANCH_TOKEN;
    delete process.env.TINYBIRD_TOKEN;
    delete process.env.TINYBIRD_URL;
    clearTokenCache();
  });

  afterEach(() => {
    process.env = { ...originalEnv };
    clearTokenCache();
    vi.restoreAllMocks();
  });

  describe("isPreviewEnvironment", () => {
    it("returns false in non-preview environment", () => {
      expect(isPreviewEnvironment()).toBe(false);
    });

    it("returns true for Vercel preview deployments", () => {
      process.env.VERCEL_ENV = "preview";
      expect(isPreviewEnvironment()).toBe(true);
    });

    it("returns false for Vercel production deployments", () => {
      process.env.VERCEL_ENV = "production";
      expect(isPreviewEnvironment()).toBe(false);
    });

    it("returns true for GitHub Actions PRs", () => {
      process.env.GITHUB_HEAD_REF = "feature-branch";
      expect(isPreviewEnvironment()).toBe(true);
    });

    it("returns true for GitLab merge requests", () => {
      process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME = "feature-branch";
      expect(isPreviewEnvironment()).toBe(true);
    });

    it("returns true for generic CI with preview mode", () => {
      process.env.CI = "true";
      process.env.TINYBIRD_PREVIEW_MODE = "true";
      expect(isPreviewEnvironment()).toBe(true);
    });

    it("returns false for generic CI without preview mode", () => {
      process.env.CI = "true";
      expect(isPreviewEnvironment()).toBe(false);
    });
  });

  describe("getPreviewBranchName", () => {
    it("returns null when no env vars are set", () => {
      expect(getPreviewBranchName()).toBeNull();
    });

    it("prefers explicit TINYBIRD_BRANCH_NAME override", () => {
      process.env.TINYBIRD_BRANCH_NAME = "override-branch";
      process.env.VERCEL_GIT_COMMIT_REF = "vercel-branch";
      expect(getPreviewBranchName()).toBe("override-branch");
    });

    it("uses VERCEL_GIT_COMMIT_REF for Vercel", () => {
      process.env.VERCEL_GIT_COMMIT_REF = "vercel-branch";
      expect(getPreviewBranchName()).toBe("vercel-branch");
    });

    it("uses GITHUB_HEAD_REF for GitHub Actions PRs", () => {
      process.env.GITHUB_HEAD_REF = "pr-branch";
      expect(getPreviewBranchName()).toBe("pr-branch");
    });

    it("uses GITHUB_REF_NAME for GitHub Actions pushes", () => {
      process.env.GITHUB_REF_NAME = "main";
      expect(getPreviewBranchName()).toBe("main");
    });

    it("prefers GITHUB_HEAD_REF over GITHUB_REF_NAME", () => {
      process.env.GITHUB_HEAD_REF = "pr-branch";
      process.env.GITHUB_REF_NAME = "main";
      expect(getPreviewBranchName()).toBe("pr-branch");
    });

    it("uses CI_MERGE_REQUEST_SOURCE_BRANCH_NAME for GitLab MRs", () => {
      process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME = "mr-branch";
      expect(getPreviewBranchName()).toBe("mr-branch");
    });

    it("uses CI_COMMIT_BRANCH for GitLab CI branches", () => {
      process.env.CI_COMMIT_BRANCH = "gitlab-branch";
      expect(getPreviewBranchName()).toBe("gitlab-branch");
    });

    it("uses CIRCLE_BRANCH for CircleCI", () => {
      process.env.CIRCLE_BRANCH = "circle-branch";
      expect(getPreviewBranchName()).toBe("circle-branch");
    });

    it("uses BUILD_SOURCEBRANCHNAME for Azure Pipelines", () => {
      process.env.BUILD_SOURCEBRANCHNAME = "azure-branch";
      expect(getPreviewBranchName()).toBe("azure-branch");
    });

    it("uses BITBUCKET_BRANCH for Bitbucket Pipelines", () => {
      process.env.BITBUCKET_BRANCH = "bitbucket-branch";
      expect(getPreviewBranchName()).toBe("bitbucket-branch");
    });
  });

  describe("resolveToken", () => {
    it("returns TINYBIRD_BRANCH_TOKEN if set", async () => {
      process.env.TINYBIRD_BRANCH_TOKEN = "branch-token";
      process.env.TINYBIRD_TOKEN = "workspace-token";
      const token = await resolveToken();
      expect(token).toBe("branch-token");
    });

    it("throws if no token is configured", async () => {
      await expect(resolveToken()).rejects.toThrow("TINYBIRD_TOKEN is not configured");
    });

    it("returns configured token from options", async () => {
      const token = await resolveToken({ token: "option-token" });
      expect(token).toBe("option-token");
    });

    it("returns TINYBIRD_TOKEN when not in preview environment", async () => {
      process.env.TINYBIRD_TOKEN = "workspace-token";
      const token = await resolveToken();
      expect(token).toBe("workspace-token");
    });

    it("passes token from options", async () => {
      const token = await resolveToken({ token: "my-token" });
      expect(token).toBe("my-token");
    });
  });

  describe("clearTokenCache", () => {
    it("clears the cached token", async () => {
      // Just verify it doesn't throw
      expect(() => clearTokenCache()).not.toThrow();
    });
  });
});
