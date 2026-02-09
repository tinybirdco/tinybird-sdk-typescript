/**
 * Tinybird Dashboard URL utilities
 *
 * Generates dashboard links for workspaces and branches
 */

/**
 * Region information extracted from API URL
 */
export interface RegionInfo {
  /** Cloud provider: "gcp" or "aws" */
  provider: string;
  /** Region identifier (e.g., "europe-west3", "us-east4", "us-west-2") */
  region: string;
}

/**
 * Mapping of API hostnames to region information
 *
 * Based on https://www.tinybird.co/docs/api-reference#current-tinybird-regions
 */
const API_REGION_MAP: Record<string, RegionInfo> = {
  // GCP Regions
  "api.tinybird.co": { provider: "gcp", region: "europe-west3" },
  "api.us-east.tinybird.co": { provider: "gcp", region: "us-east4" },
  // AWS Regions
  "api.eu-central-1.aws.tinybird.co": { provider: "aws", region: "eu-central-1" },
  "api.us-east-1.aws.tinybird.co": { provider: "aws", region: "us-east-1" },
  "api.us-west-2.aws.tinybird.co": { provider: "aws", region: "us-west-2" },
};

/**
 * Parse an API URL to extract region information
 *
 * @param apiUrl - The Tinybird API base URL (e.g., "https://api.tinybird.co")
 * @returns Region info or null if the URL doesn't match a known region
 *
 * @example
 * ```ts
 * parseApiUrl("https://api.tinybird.co")
 * // => { provider: "gcp", region: "europe-west3" }
 *
 * parseApiUrl("https://api.us-west-2.aws.tinybird.co")
 * // => { provider: "aws", region: "us-west-2" }
 * ```
 */
export function parseApiUrl(apiUrl: string): RegionInfo | null {
  try {
    const url = new URL(apiUrl);
    const hostname = url.hostname;
    return API_REGION_MAP[hostname] ?? null;
  } catch {
    return null;
  }
}

/**
 * Generate a Tinybird dashboard URL for a workspace
 *
 * @param apiUrl - The Tinybird API base URL
 * @param workspaceName - The workspace name
 * @returns Dashboard URL or null if the API URL is not recognized
 *
 * @example
 * ```ts
 * getDashboardUrl("https://api.tinybird.co", "my_workspace")
 * // => "https://cloud.tinybird.co/gcp/europe-west3/my_workspace"
 * ```
 */
export function getDashboardUrl(apiUrl: string, workspaceName: string): string | null {
  const regionInfo = parseApiUrl(apiUrl);
  if (!regionInfo) {
    return null;
  }

  return `https://cloud.tinybird.co/${regionInfo.provider}/${regionInfo.region}/${workspaceName}`;
}

/**
 * Generate a Tinybird dashboard URL for a branch
 *
 * @param apiUrl - The Tinybird API base URL
 * @param workspaceName - The workspace name
 * @param branchName - The branch name
 * @returns Dashboard URL or null if the API URL is not recognized
 *
 * @example
 * ```ts
 * getBranchDashboardUrl("https://api.tinybird.co", "my_workspace", "feature_branch")
 * // => "https://cloud.tinybird.co/gcp/europe-west3/my_workspace~feature_branch"
 * ```
 */
export function getBranchDashboardUrl(
  apiUrl: string,
  workspaceName: string,
  branchName: string
): string | null {
  const regionInfo = parseApiUrl(apiUrl);
  if (!regionInfo) {
    return null;
  }

  return `https://cloud.tinybird.co/${regionInfo.provider}/${regionInfo.region}/${workspaceName}~${branchName}`;
}

/**
 * Generate a local Tinybird dashboard URL
 *
 * @param workspaceName - The local workspace name
 * @param port - The local Tinybird port (default: 7181)
 * @returns Local dashboard URL
 *
 * @example
 * ```ts
 * getLocalDashboardUrl("my_local_workspace")
 * // => "https://cloud.tinybird.co/local/7181/my_local_workspace"
 * ```
 */
export function getLocalDashboardUrl(workspaceName: string, port = 7181): string {
  return `https://cloud.tinybird.co/local/${port}/${workspaceName}`;
}
