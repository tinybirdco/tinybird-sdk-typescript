/**
 * Region selection utility for CLI commands
 *
 * Provides interactive region selection using @clack/prompts
 */

import * as fs from "fs";
import * as p from "@clack/prompts";
import { fetchRegions, type TinybirdRegion } from "../api/regions.js";

/**
 * Default fallback regions if API call fails
 */
const FALLBACK_REGIONS: TinybirdRegion[] = [
  {
    name: "EU (GCP)",
    api_host: "https://api.europe-west2.gcp.tinybird.co",
    provider: "gcp",
  },
  {
    name: "US East (AWS)",
    api_host: "https://api.us-east.aws.tinybird.co",
    provider: "aws",
  },
  {
    name: "EU (Default)",
    api_host: "https://api.tinybird.co",
    provider: "gcp",
  },
];

/**
 * Result of region selection
 */
export interface RegionSelectionResult {
  /** Whether selection was successful (not cancelled) */
  success: boolean;
  /** Selected API host URL */
  apiHost?: string;
  /** Selected region name (for display) */
  regionName?: string;
  /** Whether user cancelled */
  cancelled?: boolean;
}

/**
 * Prompt user to select a Tinybird region
 *
 * Fetches available regions from the API and presents an interactive selection.
 * Falls back to hardcoded regions if the API call fails.
 *
 * @param defaultApiHost - Optional API host to pre-select in the prompt
 * @returns Selected region info or cancellation result
 */
export async function selectRegion(
  defaultApiHost?: string
): Promise<RegionSelectionResult> {
  let regions: TinybirdRegion[];

  // Try to fetch regions from API
  try {
    regions = await fetchRegions();
  } catch {
    // Fall back to hardcoded regions
    regions = FALLBACK_REGIONS;
  }

  // Ensure we have at least one region
  if (regions.length === 0) {
    regions = FALLBACK_REGIONS;
  }

  // Build options for p.select
  const options = regions.map((region) => ({
    value: region.api_host,
    label: region.name,
    hint: region.api_host.replace("https://", ""),
  }));

  // Find initial value if defaultApiHost is provided and matches a region
  // Normalize URLs for comparison (remove trailing slashes, lowercase)
  const normalizeUrl = (url: string) => url.toLowerCase().replace(/\/+$/, "");
  const initialValue = defaultApiHost
    ? regions.find(
        (r) => normalizeUrl(r.api_host) === normalizeUrl(defaultApiHost)
      )?.api_host
    : undefined;

  const selected = await p.select({
    message: "Select your Tinybird region",
    options,
    initialValue,
  });

  if (p.isCancel(selected)) {
    p.cancel("Operation cancelled");
    return {
      success: false,
      cancelled: true,
    };
  }

  const selectedRegion = regions.find((r) => r.api_host === selected);

  return {
    success: true,
    apiHost: selected as string,
    regionName: selectedRegion?.name,
  };
}

/**
 * Get API host from config file or prompt for region selection
 *
 * @param configPath - Path to config file (or null if no config)
 * @returns API host URL and source, or null if cancelled
 */
export async function getApiHostWithRegionSelection(
  configPath: string | null
): Promise<{ apiHost: string; fromConfig: boolean } | null> {
  let existingBaseUrl: string | undefined;

  // If we have a JSON config file, try to read baseUrl from it
  if (configPath && configPath.endsWith(".json")) {
    try {
      const content = fs.readFileSync(configPath, "utf-8");
      const config = JSON.parse(content);
      existingBaseUrl = config.baseUrl;
    } catch {
      // Ignore errors reading config
    }
  }

  // Prompt for region selection, pre-selecting existing baseUrl if available
  const result = await selectRegion(existingBaseUrl);

  if (!result.success || !result.apiHost) {
    return null;
  }

  return {
    apiHost: result.apiHost,
    fromConfig: false,
  };
}
