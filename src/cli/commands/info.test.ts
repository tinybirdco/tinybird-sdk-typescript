import { describe, it, expect, beforeEach, vi } from "vitest";
import { runInfo } from "./info.js";

// Mock the config module
vi.mock("../config.js", () => ({
  loadConfig: vi.fn(),
  LOCAL_BASE_URL: "http://localhost:7181",
}));

// Mock the API modules
vi.mock("../../api/workspaces.js", () => ({
  getWorkspace: vi.fn(),
}));

vi.mock("../../api/branches.js", () => ({
  listBranches: vi.fn(),
  getBranch: vi.fn(),
}));

vi.mock("../../api/dashboard.js", () => ({
  getDashboardUrl: vi.fn(),
  getBranchDashboardUrl: vi.fn(),
  getLocalDashboardUrl: vi.fn(),
}));

vi.mock("../../api/local.js", () => ({
  isLocalRunning: vi.fn(),
  getLocalTokens: vi.fn(),
  getOrCreateLocalWorkspace: vi.fn(),
  getLocalWorkspaceName: vi.fn(),
}));

// Import mocked functions
import { loadConfig } from "../config.js";
import { getWorkspace } from "../../api/workspaces.js";
import { listBranches, getBranch } from "../../api/branches.js";
import { getDashboardUrl, getBranchDashboardUrl, getLocalDashboardUrl } from "../../api/dashboard.js";
import {
  isLocalRunning,
  getLocalTokens,
  getOrCreateLocalWorkspace,
  getLocalWorkspaceName,
} from "../../api/local.js";

const mockedLoadConfig = vi.mocked(loadConfig);
const mockedGetWorkspace = vi.mocked(getWorkspace);
const mockedListBranches = vi.mocked(listBranches);
const mockedGetBranch = vi.mocked(getBranch);
const mockedGetDashboardUrl = vi.mocked(getDashboardUrl);
const mockedGetBranchDashboardUrl = vi.mocked(getBranchDashboardUrl);
const mockedGetLocalDashboardUrl = vi.mocked(getLocalDashboardUrl);
const mockedIsLocalRunning = vi.mocked(isLocalRunning);
const mockedGetLocalTokens = vi.mocked(getLocalTokens);
const mockedGetOrCreateLocalWorkspace = vi.mocked(getOrCreateLocalWorkspace);
const mockedGetLocalWorkspaceName = vi.mocked(getLocalWorkspaceName);

describe("Info Command", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("config loading", () => {
    it("returns error when config loading fails", async () => {
      mockedLoadConfig.mockImplementation(() => {
        throw new Error("No tinybird.json found");
      });

      const result = await runInfo();

      expect(result.success).toBe(false);
      expect(result.error).toContain("No tinybird.json found");
    });
  });

  describe("branch mode", () => {
    const mockConfig = {
      cwd: "/test/project",
      configPath: "/test/project/tinybird.json",
      devMode: "branch" as const,
      gitBranch: "feature/test",
      tinybirdBranch: "feature_test",
      isMainBranch: false,
      baseUrl: "https://api.tinybird.co",
      token: "test-token",
    };

    const mockWorkspace = {
      id: "ws-123",
      name: "test-workspace",
      user_email: "user@example.com",
      user_id: "user-123",
      scope: "WORKSPACE",
      main: null,
    };

    beforeEach(() => {
      mockedLoadConfig.mockReturnValue(mockConfig);
      mockedGetWorkspace.mockResolvedValue(mockWorkspace);
      mockedListBranches.mockResolvedValue([]);
      mockedGetDashboardUrl.mockReturnValue("https://cloud.tinybird.co/gcp/europe-west3/test-workspace");
    });

    it("returns cloud info in branch mode", async () => {
      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.cloud).toEqual({
        workspaceName: "test-workspace",
        workspaceId: "ws-123",
        userEmail: "user@example.com",
        apiHost: "https://api.tinybird.co",
        dashboardUrl: "https://cloud.tinybird.co/gcp/europe-west3/test-workspace",
        token: "test-token",
      });
    });

    it("does not return local info in branch mode", async () => {
      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.local).toBeUndefined();
      expect(mockedIsLocalRunning).not.toHaveBeenCalled();
    });

    it("returns project info", async () => {
      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.project).toEqual({
        cwd: "/test/project",
        configPath: "/test/project/tinybird.json",
        devMode: "branch",
        gitBranch: "feature/test",
        tinybirdBranch: "feature_test",
        isMainBranch: false,
      });
    });

    it("returns branch info when on a feature branch", async () => {
      const mockBranch = {
        id: "branch-123",
        name: "feature_test",
        token: "branch-token",
        created_at: "2024-01-01",
      };
      mockedGetBranch.mockResolvedValue(mockBranch);
      mockedGetBranchDashboardUrl.mockReturnValue(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace~feature_test"
      );

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.branch).toEqual({
        name: "feature_test",
        id: "branch-123",
        token: "branch-token",
        dashboardUrl: "https://cloud.tinybird.co/gcp/europe-west3/test-workspace~feature_test",
      });
    });

    it("does not return branch info when on main branch", async () => {
      mockedLoadConfig.mockReturnValue({
        ...mockConfig,
        gitBranch: "main",
        tinybirdBranch: null,
        isMainBranch: true,
      });

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.branch).toBeUndefined();
      expect(mockedGetBranch).not.toHaveBeenCalled();
    });

    it("returns branches list in branch mode", async () => {
      const mockBranches = [
        { id: "b1", name: "branch1", created_at: "2024-01-01" },
        { id: "b2", name: "branch2", created_at: "2024-01-02" },
      ];
      mockedListBranches.mockResolvedValue(mockBranches);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.branches).toEqual(mockBranches);
    });
  });

  describe("local mode", () => {
    const mockConfig = {
      cwd: "/test/project",
      configPath: "/test/project/tinybird.json",
      devMode: "local" as const,
      gitBranch: "feature/test",
      tinybirdBranch: "feature_test",
      isMainBranch: false,
      baseUrl: "https://api.tinybird.co",
      token: "test-token",
    };

    const mockWorkspace = {
      id: "ws-123",
      name: "test-workspace",
      user_email: "user@example.com",
      user_id: "user-123",
      scope: "WORKSPACE",
      main: null,
    };

    beforeEach(() => {
      mockedLoadConfig.mockReturnValue(mockConfig);
      mockedGetWorkspace.mockResolvedValue(mockWorkspace);
      mockedGetDashboardUrl.mockReturnValue("https://cloud.tinybird.co/gcp/europe-west3/test-workspace");
    });

    it("returns cloud info in local mode", async () => {
      mockedIsLocalRunning.mockResolvedValue(false);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.cloud).toBeDefined();
      expect(result.cloud?.workspaceName).toBe("test-workspace");
    });

    it("returns local info when local is running", async () => {
      mockedIsLocalRunning.mockResolvedValue(true);
      mockedGetLocalTokens.mockResolvedValue({
        user_token: "local-user-token",
        admin_token: "local-admin-token",
        workspace_admin_token: "local-ws-admin-token",
      });
      mockedGetLocalWorkspaceName.mockReturnValue("local_workspace");
      mockedGetOrCreateLocalWorkspace.mockResolvedValue({
        workspace: {
          id: "local-ws-123",
          name: "local_workspace",
          token: "local-token",
        },
        wasCreated: false,
      });
      mockedGetLocalDashboardUrl.mockReturnValue("https://cloud.tinybird.co/local/7181/local_workspace");

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.local).toEqual({
        running: true,
        workspaceName: "local_workspace",
        workspaceId: "local-ws-123",
        apiHost: "http://localhost:7181",
        dashboardUrl: "https://cloud.tinybird.co/local/7181/local_workspace",
        token: "local-token",
      });
    });

    it("returns local info with running=false when local is not running", async () => {
      mockedIsLocalRunning.mockResolvedValue(false);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.local).toEqual({
        running: false,
        apiHost: "http://localhost:7181",
      });
    });

    it("does not return branch info in local mode", async () => {
      mockedIsLocalRunning.mockResolvedValue(false);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.branch).toBeUndefined();
      expect(result.branches).toEqual([]);
      expect(mockedGetBranch).not.toHaveBeenCalled();
      expect(mockedListBranches).not.toHaveBeenCalled();
    });
  });

  describe("error handling", () => {
    it("returns error when workspace fetch fails", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "branch" as const,
        gitBranch: null,
        tinybirdBranch: null,
        isMainBranch: true,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });
      mockedGetWorkspace.mockRejectedValue(new Error("Unauthorized"));

      const result = await runInfo();

      expect(result.success).toBe(false);
      expect(result.error).toContain("Failed to get workspace info");
      expect(result.error).toContain("Unauthorized");
    });

    it("handles branch fetch error gracefully", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "branch" as const,
        gitBranch: "feature/test",
        tinybirdBranch: "feature_test",
        isMainBranch: false,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });
      mockedGetWorkspace.mockResolvedValue({
        id: "ws-123",
        name: "test-workspace",
        user_email: "user@example.com",
        user_id: "user-123",
        scope: "WORKSPACE",
        main: null,
      });
      mockedGetBranch.mockRejectedValue(new Error("Branch not found"));
      mockedListBranches.mockResolvedValue([]);
      mockedGetDashboardUrl.mockReturnValue(null);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.branch).toBeUndefined();
    });

    it("handles branches list fetch error gracefully", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "branch" as const,
        gitBranch: "main",
        tinybirdBranch: null,
        isMainBranch: true,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });
      mockedGetWorkspace.mockResolvedValue({
        id: "ws-123",
        name: "test-workspace",
        user_email: "user@example.com",
        user_id: "user-123",
        scope: "WORKSPACE",
        main: null,
      });
      mockedListBranches.mockRejectedValue(new Error("Network error"));
      mockedGetDashboardUrl.mockReturnValue(null);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.branches).toEqual([]);
    });

    it("handles local workspace fetch error gracefully", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "local" as const,
        gitBranch: "feature/test",
        tinybirdBranch: "feature_test",
        isMainBranch: false,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });
      mockedGetWorkspace.mockResolvedValue({
        id: "ws-123",
        name: "test-workspace",
        user_email: "user@example.com",
        user_id: "user-123",
        scope: "WORKSPACE",
        main: null,
      });
      mockedIsLocalRunning.mockResolvedValue(true);
      mockedGetLocalTokens.mockRejectedValue(new Error("Connection refused"));
      mockedGetDashboardUrl.mockReturnValue(null);

      const result = await runInfo();

      expect(result.success).toBe(true);
      expect(result.local).toEqual({
        running: true,
        apiHost: "http://localhost:7181",
      });
    });
  });
});
