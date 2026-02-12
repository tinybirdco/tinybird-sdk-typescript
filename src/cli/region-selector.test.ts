import { describe, it, expect, vi, beforeEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { selectRegion, getApiHostWithRegionSelection } from "./region-selector.js";

// Mock the regions API
vi.mock("../api/regions.js", () => ({
  fetchRegions: vi.fn(),
}));

// Mock @clack/prompts
vi.mock("@clack/prompts", () => ({
  select: vi.fn(),
  isCancel: vi.fn(),
  cancel: vi.fn(),
}));

import { fetchRegions } from "../api/regions.js";
import * as p from "@clack/prompts";

const mockedFetchRegions = vi.mocked(fetchRegions);
const mockedSelect = vi.mocked(p.select);
const mockedIsCancel = vi.mocked(p.isCancel);

describe("selectRegion", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockedIsCancel.mockReturnValue(false);
  });

  it("fetches regions and shows selection prompt", async () => {
    mockedFetchRegions.mockResolvedValue([
      { name: "EU (GCP)", api_host: "https://api.eu.tinybird.co", provider: "gcp" },
      { name: "US (AWS)", api_host: "https://api.us.tinybird.co", provider: "aws" },
    ]);
    mockedSelect.mockResolvedValue("https://api.eu.tinybird.co");

    const result = await selectRegion();

    expect(result.success).toBe(true);
    expect(result.apiHost).toBe("https://api.eu.tinybird.co");
    expect(result.regionName).toBe("EU (GCP)");
    expect(mockedSelect).toHaveBeenCalledWith({
      message: "Select your Tinybird region",
      options: [
        { value: "https://api.eu.tinybird.co", label: "EU (GCP)", hint: "api.eu.tinybird.co" },
        { value: "https://api.us.tinybird.co", label: "US (AWS)", hint: "api.us.tinybird.co" },
      ],
    });
  });

  it("uses fallback regions when API fails", async () => {
    mockedFetchRegions.mockRejectedValue(new Error("Network error"));
    mockedSelect.mockResolvedValue("https://api.europe-west2.gcp.tinybird.co");

    const result = await selectRegion();

    expect(result.success).toBe(true);
    expect(result.apiHost).toBe("https://api.europe-west2.gcp.tinybird.co");
    expect(mockedSelect).toHaveBeenCalled();
  });

  it("returns cancelled when user cancels", async () => {
    mockedFetchRegions.mockResolvedValue([
      { name: "EU", api_host: "https://api.eu.tinybird.co", provider: "gcp" },
    ]);
    mockedSelect.mockResolvedValue(Symbol("cancel"));
    mockedIsCancel.mockReturnValue(true);

    const result = await selectRegion();

    expect(result.success).toBe(false);
    expect(result.cancelled).toBe(true);
  });

  it("uses fallback regions when API returns empty array", async () => {
    mockedFetchRegions.mockResolvedValue([]);
    mockedSelect.mockResolvedValue("https://api.tinybird.co");

    const result = await selectRegion();

    expect(result.success).toBe(true);
    // Should have used fallback regions
    expect(mockedSelect).toHaveBeenCalled();
  });
});

describe("getApiHostWithRegionSelection", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "region-selector-test-"));
    vi.clearAllMocks();
    mockedIsCancel.mockReturnValue(false);
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  it("returns baseUrl from config when present", async () => {
    const configPath = path.join(tempDir, "tinybird.config.json");
    fs.writeFileSync(
      configPath,
      JSON.stringify({ baseUrl: "https://api.us-east.tinybird.co" })
    );

    const result = await getApiHostWithRegionSelection(configPath);

    expect(result).toEqual({
      apiHost: "https://api.us-east.tinybird.co",
      fromConfig: true,
    });
    expect(mockedSelect).not.toHaveBeenCalled();
  });

  it("prompts for region when config has no baseUrl", async () => {
    const configPath = path.join(tempDir, "tinybird.config.json");
    fs.writeFileSync(configPath, JSON.stringify({ token: "test" }));

    mockedFetchRegions.mockResolvedValue([
      { name: "EU", api_host: "https://api.eu.tinybird.co", provider: "gcp" },
    ]);
    mockedSelect.mockResolvedValue("https://api.eu.tinybird.co");

    const result = await getApiHostWithRegionSelection(configPath);

    expect(result).toEqual({
      apiHost: "https://api.eu.tinybird.co",
      fromConfig: false,
    });
    expect(mockedSelect).toHaveBeenCalled();
  });

  it("prompts for region when config file is null", async () => {
    mockedFetchRegions.mockResolvedValue([
      { name: "EU", api_host: "https://api.eu.tinybird.co", provider: "gcp" },
    ]);
    mockedSelect.mockResolvedValue("https://api.eu.tinybird.co");

    const result = await getApiHostWithRegionSelection(null);

    expect(result).toEqual({
      apiHost: "https://api.eu.tinybird.co",
      fromConfig: false,
    });
  });

  it("prompts for region when config is not JSON", async () => {
    mockedFetchRegions.mockResolvedValue([
      { name: "EU", api_host: "https://api.eu.tinybird.co", provider: "gcp" },
    ]);
    mockedSelect.mockResolvedValue("https://api.eu.tinybird.co");

    const result = await getApiHostWithRegionSelection("/path/to/config.mjs");

    expect(result).toEqual({
      apiHost: "https://api.eu.tinybird.co",
      fromConfig: false,
    });
  });

  it("returns null when user cancels region selection", async () => {
    const configPath = path.join(tempDir, "tinybird.config.json");
    fs.writeFileSync(configPath, JSON.stringify({ token: "test" }));

    mockedFetchRegions.mockResolvedValue([
      { name: "EU", api_host: "https://api.eu.tinybird.co", provider: "gcp" },
    ]);
    mockedSelect.mockResolvedValue(Symbol("cancel"));
    mockedIsCancel.mockReturnValue(true);

    const result = await getApiHostWithRegionSelection(configPath);

    expect(result).toBeNull();
  });
});
