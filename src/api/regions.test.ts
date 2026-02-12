import { describe, it, expect, vi, beforeEach } from "vitest";
import { fetchRegions, RegionsApiError } from "./regions.js";

// Mock the fetcher module
vi.mock("./fetcher.js", () => ({
  tinybirdFetch: vi.fn(),
}));

import { tinybirdFetch } from "./fetcher.js";

const mockedFetch = vi.mocked(tinybirdFetch);

describe("fetchRegions", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns regions from API", async () => {
    mockedFetch.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          regions: [
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
          ],
        }),
    } as Response);

    const regions = await fetchRegions();

    expect(regions).toHaveLength(2);
    expect(regions[0].name).toBe("EU (GCP)");
    expect(regions[0].api_host).toBe("https://api.europe-west2.gcp.tinybird.co");
    expect(regions[1].provider).toBe("aws");
  });

  it("calls the correct endpoint", async () => {
    mockedFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ regions: [] }),
    } as Response);

    await fetchRegions();

    expect(mockedFetch).toHaveBeenCalledWith(
      "https://api.tinybird.co/v0/regions",
      { method: "GET" }
    );
  });

  it("throws RegionsApiError on non-ok response", async () => {
    mockedFetch.mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
      text: () => Promise.resolve("Server error"),
    } as Response);

    await expect(fetchRegions()).rejects.toThrow(RegionsApiError);
    await expect(fetchRegions()).rejects.toThrow("Failed to fetch regions: 500");
  });

  it("throws RegionsApiError on network error", async () => {
    mockedFetch.mockRejectedValue(new Error("Network error"));

    await expect(fetchRegions()).rejects.toThrow(RegionsApiError);
    await expect(fetchRegions()).rejects.toThrow("Network error");
  });

  it("returns empty array when API returns empty regions", async () => {
    mockedFetch.mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ regions: [] }),
    } as Response);

    const regions = await fetchRegions();

    expect(regions).toEqual([]);
  });
});
