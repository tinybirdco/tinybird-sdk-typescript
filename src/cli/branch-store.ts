/**
 * Branch token storage in ~/.tinybird/branches.json
 */

import * as fs from "fs";
import * as path from "path";
import * as os from "os";

/**
 * Information about a cached branch
 */
export interface BranchInfo {
  /** Branch ID from Tinybird */
  id: string;
  /** Branch token for API access */
  token: string;
  /** When the branch was created/cached */
  createdAt: string;
}

/**
 * Structure of the branches.json file
 */
export interface BranchStore {
  workspaces: Record<
    string,
    {
      branches: Record<string, BranchInfo>;
    }
  >;
}

/**
 * Get the path to the branches.json file
 */
export function getBranchStorePath(): string {
  return path.join(os.homedir(), ".tinybird", "branches.json");
}

/**
 * Ensure the ~/.tinybird directory exists
 */
function ensureTinybirdDir(): void {
  const tinybirdDir = path.join(os.homedir(), ".tinybird");
  if (!fs.existsSync(tinybirdDir)) {
    try {
      fs.mkdirSync(tinybirdDir, { recursive: true });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(
        `Failed to create Tinybird config directory at ${tinybirdDir}: ${message}. ` +
          `Please ensure you have write permissions to your home directory.`
      );
    }
  }
}

/**
 * Load the branch store from disk
 * Returns an empty store if the file doesn't exist
 */
export function loadBranchStore(): BranchStore {
  const storePath = getBranchStorePath();

  if (!fs.existsSync(storePath)) {
    return { workspaces: {} };
  }

  try {
    const content = fs.readFileSync(storePath, "utf-8");
    return JSON.parse(content) as BranchStore;
  } catch {
    // If the file is corrupted, return empty store
    return { workspaces: {} };
  }
}

/**
 * Save the branch store to disk
 */
export function saveBranchStore(store: BranchStore): void {
  ensureTinybirdDir();
  const storePath = getBranchStorePath();
  fs.writeFileSync(storePath, JSON.stringify(store, null, 2), "utf-8");
}

/**
 * Get a cached branch token
 * Returns null if not cached
 */
export function getBranchToken(
  workspaceId: string,
  branchName: string
): BranchInfo | null {
  const store = loadBranchStore();
  return store.workspaces[workspaceId]?.branches[branchName] ?? null;
}

/**
 * Cache a branch token
 */
export function setBranchToken(
  workspaceId: string,
  branchName: string,
  info: BranchInfo
): void {
  const store = loadBranchStore();

  if (!store.workspaces[workspaceId]) {
    store.workspaces[workspaceId] = { branches: {} };
  }

  store.workspaces[workspaceId].branches[branchName] = info;
  saveBranchStore(store);
}

/**
 * Remove a cached branch
 */
export function removeBranch(workspaceId: string, branchName: string): void {
  const store = loadBranchStore();

  if (store.workspaces[workspaceId]?.branches[branchName]) {
    delete store.workspaces[workspaceId].branches[branchName];
    saveBranchStore(store);
  }
}

/**
 * List all cached branches for a workspace
 */
export function listCachedBranches(
  workspaceId: string
): Record<string, BranchInfo> {
  const store = loadBranchStore();
  return store.workspaces[workspaceId]?.branches ?? {};
}
