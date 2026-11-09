import { listBranches, deleteBranch, BranchApiError } from "../src/api/branches.js";
import { getLiveE2EConfigFromEnv } from "./cloud-workspace.js";

/**
 * Branch names created by the live suites. GITHUB_REF_NAME is set to
 * `live-<suite>/<run-scoped name>`, which the SDK sanitizes to `live_<suite>_...`.
 */
const LIVE_BRANCH_PREFIX = "live_";

/**
 * All live E2E runs share one workspace and CI serializes them, so any `live_*`
 * branch present at startup was left behind by a cancelled or crashed run.
 * Remove them so they never count against the workspace's branch limit.
 */
export default async function setup(): Promise<void> {
  const config = getLiveE2EConfigFromEnv();
  if (!config) {
    return;
  }

  const apiConfig = { baseUrl: config.baseUrl, token: config.workspaceToken };
  const branches = await listBranches(apiConfig);
  const stale = branches.filter((branch) => branch.name.startsWith(LIVE_BRANCH_PREFIX));

  for (const branch of stale) {
    try {
      await deleteBranch(apiConfig, branch.name);
      console.log(`[e2e-live] Deleted leftover branch ${branch.name}`);
    } catch (error) {
      if (!(error instanceof BranchApiError && error.status === 404)) {
        throw error;
      }
    }
  }
}
