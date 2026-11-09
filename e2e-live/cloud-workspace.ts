import { loadEnvFiles } from "../src/cli/config.js";
import { runDeploy } from "../src/cli/commands/deploy.js";

/**
 * Live E2E suites run against a single, dedicated CI workspace.
 *
 * - TINYBIRD_E2E_WORKSPACE_TOKEN: the workspace's admin token. Used for branch
 *   operations (create/get/delete require ADMIN scope), ingestion, queries and pull.
 * - TINYBIRD_E2E_DEPLOY_TOKEN: a static token with WORKSPACE:DEPLOY scope, used for
 *   production deployments. Optional locally; falls back to the workspace token.
 *
 * No user tokens are needed: suites never create or delete workspaces.
 */
export interface LiveE2EConfig {
  baseUrl: string;
  workspaceToken: string;
  deployToken: string;
}

// Load env from repository root where tinybird.config.* usually lives.
loadEnvFiles(process.cwd());

export function getLiveE2EConfigFromEnv(): LiveE2EConfig | null {
  const workspaceToken = process.env.TINYBIRD_E2E_WORKSPACE_TOKEN;
  const baseUrl = process.env.TINYBIRD_E2E_BASE_URL ?? "https://api.tinybird.co";

  if (!workspaceToken) {
    return null;
  }

  return {
    baseUrl,
    workspaceToken,
    deployToken: process.env.TINYBIRD_E2E_DEPLOY_TOKEN || workspaceToken,
  };
}

/**
 * Unique, branch-safe identifier for this run so concurrent CI runs sharing the
 * workspace never collide on branch names.
 */
export function createRunScopedName(prefix: string): string {
  const runId = process.env.GITHUB_RUN_ID ?? "local";
  const suffix = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${runId}_${suffix}`.replace(/[^a-zA-Z0-9_]/g, "_");
}

/**
 * Deploy to production with the WORKSPACE:DEPLOY token, then switch back to the
 * workspace admin token for ingestion, queries and pull. The workspace is shared
 * across suites and runs, so destructive operations are allowed: each deploy
 * replaces whatever the previous one left behind.
 */
export async function deployToProduction(config: LiveE2EConfig, cwd: string) {
  process.env.TINYBIRD_TOKEN = config.deployToken;
  try {
    return await runDeploy({ cwd, allowDestructiveOperations: true });
  } finally {
    process.env.TINYBIRD_TOKEN = config.workspaceToken;
  }
}
