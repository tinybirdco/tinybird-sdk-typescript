import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  BranchApiError,
  createBranch,
  listBranches,
  getBranch,
  deleteBranch,
  branchExists,
  getOrCreateBranch,
  type BranchApiConfig,
} from "./branches.js";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

describe("Branch API client", () => {
  const config: BranchApiConfig = {
    baseUrl: "https://api.tinybird.co",
    token: "p.test-token",
  };

  beforeEach(() => {
    mockFetch.mockReset();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("createBranch", () => {
    it("creates a branch and returns it", async () => {
      const mockBranch = {
        id: "branch-123",
        name: "my-feature",
        token: "p.branch-token",
        created_at: "2024-01-01T00:00:00Z",
      };

      // 1. POST to /v1/environments returns a job
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          job: { id: "job-123", status: "waiting" },
          workspace: { id: "ws-123" },
        }),
      });

      // 2. Poll job - returns done
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ id: "job-123", status: "done" }),
      });

      // 3. Get branch with token
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockBranch),
      });

      const result = await createBranch(config, "my-feature");

      expect(mockFetch).toHaveBeenCalledTimes(3);
      expect(mockFetch).toHaveBeenNthCalledWith(
        1,
        "https://api.tinybird.co/v1/environments?name=my-feature&from=ts-sdk",
        {
          method: "POST",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
      expect(mockFetch).toHaveBeenNthCalledWith(
        2,
        "https://api.tinybird.co/v0/jobs/job-123?from=ts-sdk",
        {
          method: "GET",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
      expect(mockFetch).toHaveBeenNthCalledWith(
        3,
        "https://api.tinybird.co/v0/environments/my-feature?with_token=true&from=ts-sdk",
        {
          method: "GET",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
      expect(result).toEqual(mockBranch);
    });

    it("polls job until done", async () => {
      const mockBranch = {
        id: "branch-123",
        name: "my-feature",
        token: "p.branch-token",
        created_at: "2024-01-01T00:00:00Z",
      };

      // 1. POST to /v1/environments returns a job
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          job: { id: "job-123", status: "waiting" },
          workspace: { id: "ws-123" },
        }),
      });

      // 2. Poll job - waiting
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ id: "job-123", status: "waiting" }),
      });

      // 3. Poll job - working
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ id: "job-123", status: "working" }),
      });

      // 4. Poll job - done
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ id: "job-123", status: "done" }),
      });

      // 5. Get branch with token
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockBranch),
      });

      // Start the async operation
      const promise = createBranch(config, "my-feature");

      // Advance timers and run all pending promises
      await vi.runAllTimersAsync();

      const result = await promise;

      expect(mockFetch).toHaveBeenCalledTimes(5);
      expect(result).toEqual(mockBranch);
    });

    it("throws BranchApiError on job error", async () => {
      // 1. POST to /v1/environments returns a job
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          job: { id: "job-123", status: "waiting" },
        }),
      });

      // 2. Poll job - error
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ id: "job-123", status: "error", error: "Something went wrong" }),
      });

      await expect(createBranch(config, "my-feature")).rejects.toThrow(
        BranchApiError
      );
    });

    it("throws BranchApiError on failure", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 400,
        statusText: "Bad Request",
        text: () => Promise.resolve("Branch already exists"),
      });

      await expect(createBranch(config, "existing")).rejects.toThrow(
        BranchApiError
      );
    });
  });

  describe("listBranches", () => {
    it("returns array of branches", async () => {
      const mockBranches = [
        { id: "1", name: "feature-a", created_at: "2024-01-01T00:00:00Z" },
        { id: "2", name: "feature-b", created_at: "2024-01-02T00:00:00Z" },
      ];

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ environments: mockBranches }),
      });

      const result = await listBranches(config);

      expect(mockFetch).toHaveBeenCalledWith(
        "https://api.tinybird.co/v1/environments?from=ts-sdk",
        {
          method: "GET",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
      expect(result).toEqual(mockBranches);
    });

    it("returns empty array when no branches", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ environments: undefined }),
      });

      const result = await listBranches(config);
      expect(result).toEqual([]);
    });
  });

  describe("getBranch", () => {
    it("returns branch with token", async () => {
      const mockBranch = {
        id: "branch-123",
        name: "my-feature",
        token: "p.branch-token",
        created_at: "2024-01-01T00:00:00Z",
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockBranch),
      });

      const result = await getBranch(config, "my-feature");

      expect(mockFetch).toHaveBeenCalledWith(
        "https://api.tinybird.co/v0/environments/my-feature?with_token=true&from=ts-sdk",
        {
          method: "GET",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
      expect(result).toEqual(mockBranch);
    });

    it("throws BranchApiError when branch not found", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve("Branch not found"),
      });

      await expect(getBranch(config, "nonexistent")).rejects.toThrow(
        BranchApiError
      );
    });
  });

  describe("deleteBranch", () => {
    it("deletes a branch successfully", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
      });

      await deleteBranch(config, "my-feature");

      expect(mockFetch).toHaveBeenCalledWith(
        "https://api.tinybird.co/v1/environments/my-feature?from=ts-sdk",
        {
          method: "DELETE",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
    });
  });

  describe("branchExists", () => {
    it("returns true when branch exists", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () =>
          Promise.resolve({
            environments: [
              { id: "1", name: "my-feature", created_at: "2024-01-01" },
            ],
          }),
      });

      const result = await branchExists(config, "my-feature");
      expect(result).toBe(true);
    });

    it("returns false when branch does not exist", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ environments: [] }),
      });

      const result = await branchExists(config, "nonexistent");
      expect(result).toBe(false);
    });

    it("throws on API error", async () => {
      mockFetch.mockRejectedValueOnce(new Error("Network error"));

      await expect(branchExists(config, "any")).rejects.toThrow("Network error");
    });
  });

  describe("getOrCreateBranch", () => {
    it("returns existing branch if found", async () => {
      const mockBranch = {
        id: "branch-123",
        name: "existing-feature",
        token: "p.branch-token",
        created_at: "2024-01-01T00:00:00Z",
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockBranch),
      });

      const result = await getOrCreateBranch(config, "existing-feature");
      expect(result).toEqual({ ...mockBranch, wasCreated: false });
    });

    it("creates branch if not found", async () => {
      const newBranch = {
        id: "branch-456",
        name: "new-feature",
        token: "p.new-token",
        created_at: "2024-01-02T00:00:00Z",
      };

      // 1. getBranch returns 404
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 404,
        statusText: "Not Found",
        text: () => Promise.resolve("Not found"),
      });

      // 2. createBranch: POST to /v1/environments returns a job
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          job: { id: "job-456", status: "waiting" },
          workspace: { id: "ws-456" },
        }),
      });

      // 3. Poll job - done
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ id: "job-456", status: "done" }),
      });

      // 4. Get branch with token
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(newBranch),
      });

      const result = await getOrCreateBranch(config, "new-feature");
      expect(result).toEqual({ ...newBranch, wasCreated: true });
      expect(mockFetch).toHaveBeenCalledTimes(4);
    });
  });
});
