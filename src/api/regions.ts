/**
 * Tinybird Regions API client
 *
 * Fetches available regions from the Tinybird API
 */

import { tinybirdFetch } from "./fetcher.js";

/**
 * Default API host used to fetch regions
 * (regions endpoint is available from any host)
 */
const DEFAULT_HOST = "https://api.tinybird.co";

/**
 * Region information from Tinybird API
 */
export interface TinybirdRegion {
  /** Region name (e.g., "EU (GCP)") */
  name: string;
  /** API host URL (e.g., "https://api.europe-west2.gcp.tinybird.co") */
  api_host: string;
  /** Cloud provider (e.g., "gcp", "aws") */
  provider: string;
}

/**
 * Error thrown by regions API operations
 */
export class RegionsApiError extends Error {
  constructor(
    message: string,
    public readonly status?: number,
    public readonly body?: unknown
  ) {
    super(message);
    this.name = "RegionsApiError";
  }
}

/**
 * Fetch available Tinybird regions
 *
 * Note: This endpoint doesn't require authentication.
 *
 * @returns Array of available regions
 */
export async function fetchRegions(): Promise<TinybirdRegion[]> {
  const url = new URL("/v0/regions", DEFAULT_HOST);

  try {
    const response = await tinybirdFetch(url.toString(), {
      method: "GET",
    });

    if (!response.ok) {
      const body = await response.text();
      throw new RegionsApiError(
        `Failed to fetch regions: ${response.status} ${response.statusText}`,
        response.status,
        body
      );
    }

    const data = (await response.json()) as { regions: TinybirdRegion[] };
    return data.regions;
  } catch (error) {
    if (error instanceof RegionsApiError) {
      throw error;
    }
    throw new RegionsApiError(
      `Failed to fetch regions: ${(error as Error).message}`
    );
  }
}
