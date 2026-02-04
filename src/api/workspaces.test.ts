import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  getWorkspace,
  WorkspaceApiError,
  type WorkspaceApiConfig,
} from "./workspaces.js";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

describe("Workspace API client", () => {
  const config: WorkspaceApiConfig = {
    baseUrl: "https://api.tinybird.co",
    token: "p.test-token",
  };

  beforeEach(() => {
    mockFetch.mockReset();
  });

  describe("getWorkspace", () => {
    it("returns workspace information", async () => {
      const mockWorkspace = {
        id: "9f42135e-3434-4d89-a90f-cb9cf74ce311",
        name: "ts_client",
        releases: [],
        user_id: "412571dd-d2e6-4b3c-87b5-b29320414e22",
        user_email: "user@example.com",
        scope: "user",
        main: null,
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve(mockWorkspace),
      });

      const result = await getWorkspace(config);

      expect(mockFetch).toHaveBeenCalledWith(
        "https://api.tinybird.co/v1/workspace?from=ts-sdk",
        {
          method: "GET",
          headers: {
            Authorization: "Bearer p.test-token",
          },
        }
      );
      expect(result).toEqual(mockWorkspace);
    });

    it("throws WorkspaceApiError on failure", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
        statusText: "Unauthorized",
        text: () => Promise.resolve("Invalid token"),
      });

      await expect(getWorkspace(config)).rejects.toThrow(WorkspaceApiError);
    });

    it("includes status code in error", async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 403,
        statusText: "Forbidden",
        text: () => Promise.resolve("Access denied"),
      });

      try {
        await getWorkspace(config);
        expect.fail("Should have thrown");
      } catch (error) {
        expect(error).toBeInstanceOf(WorkspaceApiError);
        expect((error as WorkspaceApiError).status).toBe(403);
      }
    });
  });
});
