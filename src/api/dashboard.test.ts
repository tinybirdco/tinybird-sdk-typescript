import { describe, it, expect } from "vitest";
import {
  parseApiUrl,
  getDashboardUrl,
  getBranchDashboardUrl,
  getLocalDashboardUrl,
} from "./dashboard.js";

describe("parseApiUrl", () => {
  it("parses EU GCP region", () => {
    const result = parseApiUrl("https://api.tinybird.co");
    expect(result).toEqual({ provider: "gcp", region: "europe-west3" });
  });

  it("parses US East GCP region", () => {
    const result = parseApiUrl("https://api.us-east.tinybird.co");
    expect(result).toEqual({ provider: "gcp", region: "us-east4" });
  });

  it("parses EU Central AWS region", () => {
    const result = parseApiUrl("https://api.eu-central-1.aws.tinybird.co");
    expect(result).toEqual({ provider: "aws", region: "eu-central-1" });
  });

  it("parses US East AWS region", () => {
    const result = parseApiUrl("https://api.us-east-1.aws.tinybird.co");
    expect(result).toEqual({ provider: "aws", region: "us-east-1" });
  });

  it("parses US West AWS region", () => {
    const result = parseApiUrl("https://api.us-west-2.aws.tinybird.co");
    expect(result).toEqual({ provider: "aws", region: "us-west-2" });
  });

  it("returns null for unknown regions", () => {
    const result = parseApiUrl("https://api.unknown.tinybird.co");
    expect(result).toBeNull();
  });

  it("returns null for invalid URLs", () => {
    const result = parseApiUrl("not-a-url");
    expect(result).toBeNull();
  });

  it("returns null for localhost", () => {
    const result = parseApiUrl("http://localhost:7181");
    expect(result).toBeNull();
  });
});

describe("getDashboardUrl", () => {
  it("generates EU GCP workspace URL", () => {
    const result = getDashboardUrl("https://api.tinybird.co", "my_workspace");
    expect(result).toBe("https://cloud.tinybird.co/gcp/europe-west3/my_workspace");
  });

  it("generates US East GCP workspace URL", () => {
    const result = getDashboardUrl("https://api.us-east.tinybird.co", "my_workspace");
    expect(result).toBe("https://cloud.tinybird.co/gcp/us-east4/my_workspace");
  });

  it("generates AWS workspace URL", () => {
    const result = getDashboardUrl("https://api.us-west-2.aws.tinybird.co", "my_workspace");
    expect(result).toBe("https://cloud.tinybird.co/aws/us-west-2/my_workspace");
  });

  it("returns null for unknown regions", () => {
    const result = getDashboardUrl("https://api.unknown.tinybird.co", "my_workspace");
    expect(result).toBeNull();
  });
});

describe("getBranchDashboardUrl", () => {
  it("generates EU GCP branch URL", () => {
    const result = getBranchDashboardUrl("https://api.tinybird.co", "my_workspace", "feature_branch");
    expect(result).toBe("https://cloud.tinybird.co/gcp/europe-west3/my_workspace~feature_branch");
  });

  it("generates US East GCP branch URL", () => {
    const result = getBranchDashboardUrl("https://api.us-east.tinybird.co", "my_workspace", "feature_branch");
    expect(result).toBe("https://cloud.tinybird.co/gcp/us-east4/my_workspace~feature_branch");
  });

  it("generates AWS branch URL", () => {
    const result = getBranchDashboardUrl("https://api.us-west-2.aws.tinybird.co", "my_workspace", "my_feature");
    expect(result).toBe("https://cloud.tinybird.co/aws/us-west-2/my_workspace~my_feature");
  });

  it("returns null for unknown regions", () => {
    const result = getBranchDashboardUrl("https://api.unknown.tinybird.co", "my_workspace", "branch");
    expect(result).toBeNull();
  });

  it("handles branch names with underscores", () => {
    const result = getBranchDashboardUrl("https://api.tinybird.co", "my_workspace", "feature_with_underscores");
    expect(result).toBe("https://cloud.tinybird.co/gcp/europe-west3/my_workspace~feature_with_underscores");
  });
});

describe("getLocalDashboardUrl", () => {
  it("generates local dashboard URL with default port", () => {
    const result = getLocalDashboardUrl("my_local_workspace");
    expect(result).toBe("https://cloud.tinybird.co/local/7181/my_local_workspace");
  });

  it("generates local dashboard URL with custom port", () => {
    const result = getLocalDashboardUrl("my_local_workspace", 8080);
    expect(result).toBe("https://cloud.tinybird.co/local/8080/my_local_workspace");
  });

  it("handles workspace names with underscores", () => {
    const result = getLocalDashboardUrl("dublin_feature_branch");
    expect(result).toBe("https://cloud.tinybird.co/local/7181/dublin_feature_branch");
  });
});
