/**
 * Preview environment detection and branch token resolution
 *
 * Automatically detects preview/CI environments and resolves the appropriate
 * Tinybird branch token for the current git branch.
 */

import { tinybirdFetch } from "../api/fetcher.js";

/**
 * Branch information with token
 */
interface BranchWithToken {
  id: string;
  name: string;
  token: string;
  created_at: string;
}

/**
 * Cached branch token to avoid repeated API calls
 */
let cachedBranchToken: string | null = null;
let cachedBranchName: string | null = null;

/**
 * Detect if we're running in a preview/CI environment
 */
export function isPreviewEnvironment(): boolean {
  return !!(
    // Vercel preview deployments
    process.env.VERCEL_ENV === "preview" ||
    // GitHub Actions (PRs)
    process.env.GITHUB_HEAD_REF ||
    // GitLab CI (merge requests)
    process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME ||
    // Generic CI with preview indicator
    (process.env.CI && process.env.TINYBIRD_PREVIEW_MODE === "true")
  );
}

/**
 * Get the current git branch name from environment variables
 * Supports various CI platforms
 */
export function getPreviewBranchName(): string | null {
  // Explicit override
  if (process.env.TINYBIRD_BRANCH_NAME) {
    return process.env.TINYBIRD_BRANCH_NAME;
  }

  // Vercel
  if (process.env.VERCEL_GIT_COMMIT_REF) {
    return process.env.VERCEL_GIT_COMMIT_REF;
  }

  // GitHub Actions (PR)
  if (process.env.GITHUB_HEAD_REF) {
    return process.env.GITHUB_HEAD_REF;
  }

  // GitHub Actions (push)
  if (process.env.GITHUB_REF_NAME) {
    return process.env.GITHUB_REF_NAME;
  }

  // GitLab CI (merge request)
  if (process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME) {
    return process.env.CI_MERGE_REQUEST_SOURCE_BRANCH_NAME;
  }

  // GitLab CI (branch)
  if (process.env.CI_COMMIT_BRANCH) {
    return process.env.CI_COMMIT_BRANCH;
  }

  // CircleCI
  if (process.env.CIRCLE_BRANCH) {
    return process.env.CIRCLE_BRANCH;
  }

  // Azure Pipelines
  if (process.env.BUILD_SOURCEBRANCHNAME) {
    return process.env.BUILD_SOURCEBRANCHNAME;
  }

  // Bitbucket Pipelines
  if (process.env.BITBUCKET_BRANCH) {
    return process.env.BITBUCKET_BRANCH;
  }

  return null;
}

/**
 * Sanitize a git branch name for use as a Tinybird branch name
 * Tinybird only accepts alphanumeric characters and underscores
 */
function sanitizeBranchName(branchName: string): string {
  return branchName
    .replace(/[^a-zA-Z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_|_$/g, "");
}

/**
 * Fetch branch token from Tinybird API
 * Looks for branches with the tmp_ci_ prefix (created by tinybird preview)
 */
async function fetchBranchToken(
  baseUrl: string,
  workspaceToken: string,
  branchName: string
): Promise<string | null> {
  const sanitizedName = sanitizeBranchName(branchName);
  // Look for the preview branch with tmp_ci_ prefix (matches what tinybird preview creates)
  const previewBranchName = `tmp_ci_${sanitizedName}`;
  const url = new URL(`/v0/environments/${encodeURIComponent(previewBranchName)}`, baseUrl);
  url.searchParams.set("with_token", "true");

  try {
    const response = await tinybirdFetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${workspaceToken}`,
      },
    });

    if (!response.ok) {
      // Branch doesn't exist or access denied
      return null;
    }

    const branch = (await response.json()) as BranchWithToken;
    return branch.token ?? null;
  } catch {
    // Network error or other issue
    return null;
  }
}

/**
 * Resolve the token to use for API calls
 *
 * Priority:
 * 1. Explicit TINYBIRD_BRANCH_TOKEN env var
 * 2. In preview environment: fetch branch token using workspace token
 * 3. Fall back to TINYBIRD_TOKEN
 *
 * @param options - Optional configuration overrides
 * @returns The resolved token to use
 */
export async function resolveToken(options?: {
  baseUrl?: string;
  token?: string;
}): Promise<string> {
  // 1. Check for explicit branch token override
  if (process.env.TINYBIRD_BRANCH_TOKEN) {
    return process.env.TINYBIRD_BRANCH_TOKEN;
  }

  // Get the configured token (workspace token)
  const configuredToken = options?.token ?? process.env.TINYBIRD_TOKEN;

  if (!configuredToken) {
    throw new Error(
      "TINYBIRD_TOKEN is not configured. Set it in your environment or pass it to createTinybirdClient()."
    );
  }

  // 2. Check if we're in a preview environment
  if (isPreviewEnvironment()) {
    const branchName = getPreviewBranchName();

    if (branchName) {
      // Check cache first
      if (cachedBranchToken && cachedBranchName === branchName) {
        return cachedBranchToken;
      }

      const baseUrl = options?.baseUrl ?? process.env.TINYBIRD_URL ?? "https://api.tinybird.co";

      // Fetch branch token
      const branchToken = await fetchBranchToken(baseUrl, configuredToken, branchName);

      if (branchToken) {
        // Cache for subsequent calls
        cachedBranchToken = branchToken;
        cachedBranchName = branchName;
        return branchToken;
      }

      // Branch doesn't exist - fall back to workspace token
      // This allows the app to still work, just using main workspace
      const expectedBranchName = `tmp_ci_${sanitizeBranchName(branchName)}`;
      console.warn(
        `[tinybird] Preview branch "${expectedBranchName}" not found. ` +
          `Run "tinybird preview" to create it. Falling back to workspace token.`
      );
    }
  }

  // 3. Fall back to configured token
  return configuredToken;
}

/**
 * Clear the cached branch token
 * Useful for testing or when switching branches
 */
export function clearTokenCache(): void {
  cachedBranchToken = null;
  cachedBranchName = null;
}
