import { describe, it, expect } from "vitest";
import { TinybirdClient, createClient } from "./base.js";

describe("TinybirdClient", () => {
  describe("constructor", () => {
    it("throws error when baseUrl is missing", () => {
      expect(() => new TinybirdClient({ baseUrl: "", token: "test-token" })).toThrow(
        "baseUrl is required"
      );
    });

    it("throws error when token is missing", () => {
      expect(
        () => new TinybirdClient({ baseUrl: "https://api.tinybird.co", token: "" })
      ).toThrow("token is required");
    });

    it("creates client with valid config", () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });
      expect(client).toBeInstanceOf(TinybirdClient);
    });

    it("normalizes baseUrl by removing trailing slash", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co/",
        token: "test-token",
      });
      const context = await client.getContext();
      expect(context.baseUrl).toBe("https://api.tinybird.co");
    });
  });

  describe("getContext", () => {
    it("returns correct context in non-devMode", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      const context = await client.getContext();

      expect(context).toEqual({
        token: "test-token",
        baseUrl: "https://api.tinybird.co",
        devMode: false,
        isBranchToken: false,
        branchName: null,
        gitBranch: null,
      });
    });

    it("returns devMode: false when devMode is not set", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      const context = await client.getContext();
      expect(context.devMode).toBe(false);
    });

    it("returns isBranchToken: false when not in devMode", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      const context = await client.getContext();
      expect(context.isBranchToken).toBe(false);
    });

    it("returns branchName: null when not in devMode", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      const context = await client.getContext();
      expect(context.branchName).toBeNull();
    });

    it("returns gitBranch: null when not in devMode", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      const context = await client.getContext();
      expect(context.gitBranch).toBeNull();
    });

    it("caches the resolved context", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      const context1 = await client.getContext();
      const context2 = await client.getContext();

      expect(context1).toBe(context2);
    });

    it("works with different baseUrl regions", async () => {
      const client = new TinybirdClient({
        baseUrl: "https://api.us-east.tinybird.co",
        token: "us-token",
      });

      const context = await client.getContext();
      expect(context.baseUrl).toBe("https://api.us-east.tinybird.co");
      expect(context.token).toBe("us-token");
    });
  });

  describe("createClient", () => {
    it("creates a TinybirdClient instance", () => {
      const client = createClient({
        baseUrl: "https://api.tinybird.co",
        token: "test-token",
      });

      expect(client).toBeInstanceOf(TinybirdClient);
    });

    it("passes config to the client correctly", async () => {
      const client = createClient({
        baseUrl: "https://api.tinybird.co",
        token: "my-token",
      });

      const context = await client.getContext();
      expect(context.token).toBe("my-token");
      expect(context.baseUrl).toBe("https://api.tinybird.co");
    });
  });
});
