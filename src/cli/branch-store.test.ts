import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import {
  loadBranchStore,
  saveBranchStore,
  getBranchToken,
  setBranchToken,
  removeBranch,
  listCachedBranches,
  type BranchStore,
} from "./branch-store.js";

describe("Branch store", () => {
  let originalHome: string | undefined;
  let testHomeDir: string;

  beforeEach(() => {
    // Create unique test directory for each test
    testHomeDir = path.join(os.tmpdir(), ".tinybird-test-" + Date.now() + "-" + Math.random().toString(36).slice(2));
    fs.mkdirSync(testHomeDir, { recursive: true });
    // Mock HOME to use test directory
    originalHome = process.env.HOME;
    process.env.HOME = testHomeDir;
  });

  afterEach(() => {
    // Restore original HOME
    if (originalHome !== undefined) {
      process.env.HOME = originalHome;
    }
    // Clean up test directory
    try {
      fs.rmSync(testHomeDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("loadBranchStore", () => {
    it("returns empty store when file does not exist", () => {
      const store = loadBranchStore();
      expect(store).toEqual({ workspaces: {} });
    });
  });

  describe("saveBranchStore and loadBranchStore", () => {
    it("round-trips store data", () => {
      const store: BranchStore = {
        workspaces: {
          ws_123: {
            branches: {
              "feature-a": {
                id: "branch-id-1",
                token: "p.token1",
                createdAt: "2024-01-01T00:00:00Z",
              },
            },
          },
        },
      };

      saveBranchStore(store);
      const loaded = loadBranchStore();
      expect(loaded).toEqual(store);
    });
  });

  describe("getBranchToken and setBranchToken", () => {
    it("returns null for non-existent branch", () => {
      const result = getBranchToken("ws_123", "non-existent");
      expect(result).toBeNull();
    });

    it("sets and gets branch token", () => {
      const info = {
        id: "branch-id-2",
        token: "p.token2",
        createdAt: "2024-01-02T00:00:00Z",
      };

      setBranchToken("ws_456", "feature-b", info);
      const result = getBranchToken("ws_456", "feature-b");

      expect(result).toEqual(info);
    });
  });

  describe("removeBranch", () => {
    it("removes a cached branch", () => {
      const info = {
        id: "branch-id-3",
        token: "p.token3",
        createdAt: "2024-01-03T00:00:00Z",
      };

      setBranchToken("ws_789", "feature-c", info);
      expect(getBranchToken("ws_789", "feature-c")).toEqual(info);

      removeBranch("ws_789", "feature-c");
      expect(getBranchToken("ws_789", "feature-c")).toBeNull();
    });

    it("does nothing for non-existent branch", () => {
      // Should not throw
      removeBranch("ws_nonexistent", "no-branch");
    });
  });

  describe("listCachedBranches", () => {
    it("returns empty object for workspace with no branches", () => {
      const result = listCachedBranches("ws_empty");
      expect(result).toEqual({});
    });

    it("returns all branches for a workspace", () => {
      const info1 = {
        id: "branch-id-4",
        token: "p.token4",
        createdAt: "2024-01-04T00:00:00Z",
      };
      const info2 = {
        id: "branch-id-5",
        token: "p.token5",
        createdAt: "2024-01-05T00:00:00Z",
      };

      setBranchToken("ws_list", "feature-d", info1);
      setBranchToken("ws_list", "feature-e", info2);

      const result = listCachedBranches("ws_list");
      expect(result).toEqual({
        "feature-d": info1,
        "feature-e": info2,
      });
    });
  });
});
