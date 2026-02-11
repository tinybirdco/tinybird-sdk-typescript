import { describe, it, expect, beforeEach, vi } from "vitest";
import { runOpenDashboard } from "./open-dashboard.js";

// Mock the config module
vi.mock("../config.js", () => ({
  loadConfig: vi.fn(),
}));

// Mock the API modules
vi.mock("../../api/workspaces.js", () => ({
  getWorkspace: vi.fn(),
}));

vi.mock("../../api/branches.js", () => ({
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

vi.mock("../auth.js", () => ({
  openBrowser: vi.fn(),
}));

// Import mocked functions
import { loadConfig } from "../config.js";
import { getWorkspace } from "../../api/workspaces.js";
import { getBranch } from "../../api/branches.js";
import {
  getDashboardUrl,
  getBranchDashboardUrl,
  getLocalDashboardUrl,
} from "../../api/dashboard.js";
import {
  isLocalRunning,
  getLocalTokens,
  getOrCreateLocalWorkspace,
  getLocalWorkspaceName,
} from "../../api/local.js";
import { openBrowser } from "../auth.js";

const mockedLoadConfig = vi.mocked(loadConfig);
const mockedGetWorkspace = vi.mocked(getWorkspace);
const mockedGetBranch = vi.mocked(getBranch);
const mockedGetDashboardUrl = vi.mocked(getDashboardUrl);
const mockedGetBranchDashboardUrl = vi.mocked(getBranchDashboardUrl);
const mockedGetLocalDashboardUrl = vi.mocked(getLocalDashboardUrl);
const mockedIsLocalRunning = vi.mocked(isLocalRunning);
const mockedGetLocalTokens = vi.mocked(getLocalTokens);
const mockedGetOrCreateLocalWorkspace = vi.mocked(getOrCreateLocalWorkspace);
const mockedGetLocalWorkspaceName = vi.mocked(getLocalWorkspaceName);
const mockedOpenBrowser = vi.mocked(openBrowser);

describe("Open Dashboard Command", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedOpenBrowser.mockResolvedValue(true);
  });

  describe("config loading", () => {
    it("returns error when config loading fails", async () => {
      mockedLoadConfig.mockImplementation(() => {
        throw new Error("No tinybird.json found");
      });

      const result = await runOpenDashboard();

      expect(result.success).toBe(false);
      expect(result.error).toContain("No tinybird.json found");
    });
  });

  describe("cloud environment", () => {
    const mockConfig = {
      cwd: "/test/project",
      configPath: "/test/project/tinybird.json",
      devMode: "branch" as const,
      gitBranch: "main",
      tinybirdBranch: null,
      isMainBranch: true,
      baseUrl: "https://api.tinybird.co",
      token: "test-token",
      include: [],
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
      mockedGetDashboardUrl.mockReturnValue(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace"
      );
    });

    it("opens cloud dashboard when environment is cloud", async () => {
      const result = await runOpenDashboard({ environment: "cloud" });

      expect(result.success).toBe(true);
      expect(result.environment).toBe("cloud");
      expect(result.url).toBe(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace"
      );
      expect(result.browserOpened).toBe(true);
      expect(mockedOpenBrowser).toHaveBeenCalledWith(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace"
      );
    });

    it("defaults to cloud when devMode is branch and on main branch", async () => {
      const result = await runOpenDashboard();

      expect(result.success).toBe(true);
      expect(result.environment).toBe("cloud");
    });
  });

  describe("branch environment", () => {
    const mockConfig = {
      cwd: "/test/project",
      configPath: "/test/project/tinybird.json",
      devMode: "branch" as const,
      gitBranch: "feature/test",
      tinybirdBranch: "feature_test",
      isMainBranch: false,
      baseUrl: "https://api.tinybird.co",
      token: "test-token",
      include: [],
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
    });

    it("opens branch dashboard when environment is branch", async () => {
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

      const result = await runOpenDashboard({ environment: "branch" });

      expect(result.success).toBe(true);
      expect(result.environment).toBe("branch");
      expect(result.url).toBe(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace~feature_test"
      );
      expect(mockedGetBranch).toHaveBeenCalledWith(
        { baseUrl: "https://api.tinybird.co", token: "test-token" },
        "feature_test"
      );
    });

    it("defaults to branch when devMode is branch and on feature branch", async () => {
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

      const result = await runOpenDashboard();

      expect(result.success).toBe(true);
      expect(result.environment).toBe("branch");
    });

    it("returns error when trying to open branch on main", async () => {
      mockedLoadConfig.mockReturnValue({
        ...mockConfig,
        gitBranch: "main",
        tinybirdBranch: null,
        isMainBranch: true,
      });

      const result = await runOpenDashboard({ environment: "branch" });

      expect(result.success).toBe(false);
      expect(result.error).toContain("not on a feature branch");
    });

    it("returns error when branch does not exist", async () => {
      mockedGetBranch.mockRejectedValue(new Error("Branch not found"));

      const result = await runOpenDashboard({ environment: "branch" });

      expect(result.success).toBe(false);
      expect(result.error).toContain("does not exist");
      expect(result.error).toContain("tinybird build");
    });
  });

  describe("local environment", () => {
    const mockConfig = {
      cwd: "/test/project",
      configPath: "/test/project/tinybird.json",
      devMode: "local" as const,
      gitBranch: "feature/test",
      tinybirdBranch: "feature_test",
      isMainBranch: false,
      baseUrl: "https://api.tinybird.co",
      token: "test-token",
      include: [],
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
    });

    it("opens local dashboard when environment is local and local is running", async () => {
      mockedIsLocalRunning.mockResolvedValue(true);
      mockedGetLocalTokens.mockResolvedValue({
        user_token: "local-user-token",
        admin_token: "local-admin-token",
        workspace_admin_token: "local-ws-admin-token",
      });
      mockedGetLocalWorkspaceName.mockReturnValue("feature_test");
      mockedGetOrCreateLocalWorkspace.mockResolvedValue({
        workspace: {
          id: "local-ws-123",
          name: "feature_test",
          token: "local-token",
        },
        wasCreated: false,
      });
      mockedGetLocalDashboardUrl.mockReturnValue(
        "https://cloud.tinybird.co/local/7181/feature_test"
      );

      const result = await runOpenDashboard({ environment: "local" });

      expect(result.success).toBe(true);
      expect(result.environment).toBe("local");
      expect(result.url).toBe(
        "https://cloud.tinybird.co/local/7181/feature_test"
      );
    });

    it("defaults to local when devMode is local", async () => {
      mockedIsLocalRunning.mockResolvedValue(true);
      mockedGetLocalTokens.mockResolvedValue({
        user_token: "local-user-token",
        admin_token: "local-admin-token",
        workspace_admin_token: "local-ws-admin-token",
      });
      mockedGetLocalWorkspaceName.mockReturnValue("feature_test");
      mockedGetOrCreateLocalWorkspace.mockResolvedValue({
        workspace: {
          id: "local-ws-123",
          name: "feature_test",
          token: "local-token",
        },
        wasCreated: false,
      });
      mockedGetLocalDashboardUrl.mockReturnValue(
        "https://cloud.tinybird.co/local/7181/feature_test"
      );

      const result = await runOpenDashboard();

      expect(result.success).toBe(true);
      expect(result.environment).toBe("local");
    });

    it("returns error when local is not running", async () => {
      mockedIsLocalRunning.mockResolvedValue(false);

      const result = await runOpenDashboard({ environment: "local" });

      expect(result.success).toBe(false);
      expect(result.error).toContain("not running");
      expect(result.error).toContain("docker run");
    });

    it("uses workspace name on main branch for local", async () => {
      mockedLoadConfig.mockReturnValue({
        ...mockConfig,
        gitBranch: "main",
        tinybirdBranch: null,
        isMainBranch: true,
      });
      mockedIsLocalRunning.mockResolvedValue(true);
      mockedGetLocalTokens.mockResolvedValue({
        user_token: "local-user-token",
        admin_token: "local-admin-token",
        workspace_admin_token: "local-ws-admin-token",
      });
      mockedGetOrCreateLocalWorkspace.mockResolvedValue({
        workspace: {
          id: "local-ws-123",
          name: "test-workspace",
          token: "local-token",
        },
        wasCreated: false,
      });
      mockedGetLocalDashboardUrl.mockReturnValue(
        "https://cloud.tinybird.co/local/7181/test-workspace"
      );

      const result = await runOpenDashboard({ environment: "local" });

      expect(result.success).toBe(true);
      expect(mockedGetOrCreateLocalWorkspace).toHaveBeenCalledWith(
        expect.anything(),
        "test-workspace"
      );
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
        include: [],
      });
      mockedGetWorkspace.mockRejectedValue(new Error("Unauthorized"));

      const result = await runOpenDashboard({ environment: "cloud" });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Failed to get workspace info");
      expect(result.error).toContain("Unauthorized");
    });

    it("returns error when dashboard URL cannot be generated", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "branch" as const,
        gitBranch: null,
        tinybirdBranch: null,
        isMainBranch: true,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
        include: [],
      });
      mockedGetWorkspace.mockResolvedValue({
        id: "ws-123",
        name: "test-workspace",
        user_email: "user@example.com",
        user_id: "user-123",
        scope: "WORKSPACE",
        main: null,
      });
      mockedGetDashboardUrl.mockReturnValue(null);

      const result = await runOpenDashboard({ environment: "cloud" });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Could not generate dashboard URL");
    });

    it("handles local workspace fetch error", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "local" as const,
        gitBranch: "feature/test",
        tinybirdBranch: "feature_test",
        isMainBranch: false,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
        include: [],
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

      const result = await runOpenDashboard({ environment: "local" });

      expect(result.success).toBe(false);
      expect(result.error).toContain("Failed to get local workspace");
    });
  });

  describe("browser opening", () => {
    it("reports when browser failed to open", async () => {
      mockedLoadConfig.mockReturnValue({
        cwd: "/test",
        configPath: "/test/tinybird.json",
        devMode: "branch" as const,
        gitBranch: null,
        tinybirdBranch: null,
        isMainBranch: true,
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
        include: [],
      });
      mockedGetWorkspace.mockResolvedValue({
        id: "ws-123",
        name: "test-workspace",
        user_email: "user@example.com",
        user_id: "user-123",
        scope: "WORKSPACE",
        main: null,
      });
      mockedGetDashboardUrl.mockReturnValue(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace"
      );
      mockedOpenBrowser.mockResolvedValue(false);

      const result = await runOpenDashboard({ environment: "cloud" });

      expect(result.success).toBe(true);
      expect(result.browserOpened).toBe(false);
      expect(result.url).toBe(
        "https://cloud.tinybird.co/gcp/europe-west3/test-workspace"
      );
    });
  });
});
