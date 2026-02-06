import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import {
  getLocalTokens,
  listLocalWorkspaces,
  createLocalWorkspace,
  getOrCreateLocalWorkspace,
  deleteLocalWorkspace,
  clearLocalWorkspace,
  isLocalRunning,
  getLocalWorkspaceName,
  LocalNotRunningError,
  LocalApiError,
} from "./local.js";
import { LOCAL_BASE_URL } from "../cli/config.js";

const server = setupServer();

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe("Local API", () => {
  describe("isLocalRunning", () => {
    it("returns true when local container is running", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/tokens`, () => {
          return HttpResponse.json({
            user_token: "user-token",
            admin_token: "admin-token",
            workspace_admin_token: "workspace-token",
          });
        })
      );

      const result = await isLocalRunning();
      expect(result).toBe(true);
    });

    it("returns false when local container is not running", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/tokens`, () => {
          return HttpResponse.error();
        })
      );

      const result = await isLocalRunning();
      expect(result).toBe(false);
    });
  });

  describe("getLocalTokens", () => {
    it("returns tokens from local container", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/tokens`, () => {
          return HttpResponse.json({
            user_token: "user-token-123",
            admin_token: "admin-token-456",
            workspace_admin_token: "workspace-token-789",
          });
        })
      );

      const tokens = await getLocalTokens();

      expect(tokens.user_token).toBe("user-token-123");
      expect(tokens.admin_token).toBe("admin-token-456");
      expect(tokens.workspace_admin_token).toBe("workspace-token-789");
    });

    it("throws LocalNotRunningError when container is not running", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/tokens`, () => {
          return HttpResponse.error();
        })
      );

      await expect(getLocalTokens()).rejects.toThrow(LocalNotRunningError);
    });

    it("throws LocalApiError when response is invalid", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/tokens`, () => {
          return HttpResponse.json({
            // Missing required fields
            user_token: "user-token",
          });
        })
      );

      await expect(getLocalTokens()).rejects.toThrow(LocalApiError);
    });
  });

  describe("listLocalWorkspaces", () => {
    it("returns list of workspaces", async () => {
      server.use(
        http.get(
          `${LOCAL_BASE_URL}/v1/user/workspaces`,
          () => {
            return HttpResponse.json({
              organization_id: "org-123",
              workspaces: [
                { id: "ws-1", name: "Workspace1", token: "token-1" },
                { id: "ws-2", name: "Workspace2", token: "token-2" },
              ],
            });
          }
        )
      );

      const result = await listLocalWorkspaces("admin-token");

      expect(result.organizationId).toBe("org-123");
      expect(result.workspaces).toHaveLength(2);
      expect(result.workspaces[0]).toEqual({
        id: "ws-1",
        name: "Workspace1",
        token: "token-1",
      });
    });

    it("throws LocalApiError on failure", async () => {
      server.use(
        http.get(
          `${LOCAL_BASE_URL}/v1/user/workspaces`,
          () => {
            return new HttpResponse("Not found", { status: 404 });
          }
        )
      );

      await expect(listLocalWorkspaces("admin-token")).rejects.toThrow(LocalApiError);
    });
  });

  describe("createLocalWorkspace", () => {
    it("creates a new workspace", async () => {
      server.use(
        http.post(`${LOCAL_BASE_URL}/v1/workspaces`, async ({ request }) => {
          const formData = await request.text();
          const params = new URLSearchParams(formData);
          return HttpResponse.json({
            id: "new-ws-id",
            name: params.get("name"),
            token: "new-ws-token",
          });
        })
      );

      const result = await createLocalWorkspace("user-token", "TestWorkspace");

      expect(result.id).toBe("new-ws-id");
      expect(result.name).toBe("TestWorkspace");
      expect(result.token).toBe("new-ws-token");
    });

    it("throws LocalApiError on failure", async () => {
      server.use(
        http.post(`${LOCAL_BASE_URL}/v1/workspaces`, () => {
          return new HttpResponse("Server error", { status: 500 });
        })
      );

      await expect(createLocalWorkspace("user-token", "TestWorkspace")).rejects.toThrow(
        LocalApiError
      );
    });
  });

  describe("getOrCreateLocalWorkspace", () => {
    const tokens = {
      user_token: "user-token",
      admin_token: "admin-token",
      workspace_admin_token: "default-token",
    };

    it("returns existing workspace if found", async () => {
      server.use(
        http.get(
          `${LOCAL_BASE_URL}/v1/user/workspaces`,
          () => {
            return HttpResponse.json({
              organization_id: "org-123",
              workspaces: [
                { id: "existing-ws", name: "MyWorkspace", token: "existing-token" },
              ],
            });
          }
        )
      );

      const result = await getOrCreateLocalWorkspace(tokens, "MyWorkspace");

      expect(result.wasCreated).toBe(false);
      expect(result.workspace.name).toBe("MyWorkspace");
      expect(result.workspace.token).toBe("existing-token");
    });

    it("creates new workspace if not found", async () => {
      let createCalled = false;

      server.use(
        http.get(
          `${LOCAL_BASE_URL}/v1/user/workspaces`,
          () => {
            // Return different response based on whether create was called
            if (createCalled) {
              return HttpResponse.json({
                organization_id: "org-123",
                workspaces: [
                  { id: "new-ws", name: "NewWorkspace", token: "new-token" },
                ],
              });
            }
            return HttpResponse.json({
              organization_id: "org-123",
              workspaces: [], // Empty initially
            });
          }
        ),
        http.post(`${LOCAL_BASE_URL}/v1/workspaces`, () => {
          createCalled = true;
          return HttpResponse.json({
            id: "new-ws",
            name: "NewWorkspace",
            token: "new-token",
          });
        })
      );

      const result = await getOrCreateLocalWorkspace(tokens, "NewWorkspace");

      expect(result.wasCreated).toBe(true);
      expect(result.workspace.name).toBe("NewWorkspace");
      expect(result.workspace.token).toBe("new-token");
    });
  });

  describe("getLocalWorkspaceName", () => {
    it("uses branch name when available", () => {
      const name = getLocalWorkspaceName("feature_branch", "/some/path");
      expect(name).toBe("feature_branch");
    });

    it("uses hash-based name when no branch", () => {
      const name = getLocalWorkspaceName(null, "/some/path");
      expect(name).toMatch(/^Build_[a-f0-9]{16}$/);
    });

    it("generates consistent hash for same path", () => {
      const name1 = getLocalWorkspaceName(null, "/same/path");
      const name2 = getLocalWorkspaceName(null, "/same/path");
      expect(name1).toBe(name2);
    });

    it("generates different hash for different paths", () => {
      const name1 = getLocalWorkspaceName(null, "/path/one");
      const name2 = getLocalWorkspaceName(null, "/path/two");
      expect(name1).not.toBe(name2);
    });
  });

  describe("deleteLocalWorkspace", () => {
    it("deletes a workspace successfully", async () => {
      server.use(
        http.delete(`${LOCAL_BASE_URL}/v1/workspaces/ws-123`, () => {
          return new HttpResponse(null, { status: 204 });
        })
      );

      await deleteLocalWorkspace("user-token", "ws-123");
      // No error means success
    });

    it("throws LocalApiError on failure", async () => {
      server.use(
        http.delete(`${LOCAL_BASE_URL}/v1/workspaces/ws-123`, () => {
          return new HttpResponse("Not found", { status: 404 });
        })
      );

      await expect(deleteLocalWorkspace("user-token", "ws-123")).rejects.toThrow(
        LocalApiError
      );
    });
  });

  describe("clearLocalWorkspace", () => {
    const tokens = {
      user_token: "user-token",
      admin_token: "admin-token",
      workspace_admin_token: "default-token",
    };

    it("clears a workspace by deleting and recreating it", async () => {
      let deleteCount = 0;
      let createCount = 0;

      server.use(
        http.get(`${LOCAL_BASE_URL}/v1/user/workspaces`, () => {
          // First call: workspace exists
          // Second call: workspace deleted
          // Third call: workspace recreated
          if (deleteCount === 0) {
            return HttpResponse.json({
              organization_id: "org-123",
              workspaces: [
                { id: "ws-123", name: "MyWorkspace", token: "old-token" },
              ],
            });
          } else if (createCount === 0) {
            return HttpResponse.json({
              organization_id: "org-123",
              workspaces: [],
            });
          } else {
            return HttpResponse.json({
              organization_id: "org-123",
              workspaces: [
                { id: "ws-456", name: "MyWorkspace", token: "new-token" },
              ],
            });
          }
        }),
        http.delete(`${LOCAL_BASE_URL}/v1/workspaces/ws-123`, () => {
          deleteCount++;
          return new HttpResponse(null, { status: 204 });
        }),
        http.post(`${LOCAL_BASE_URL}/v1/workspaces`, () => {
          createCount++;
          return HttpResponse.json({
            id: "ws-456",
            name: "MyWorkspace",
            token: "new-token",
          });
        })
      );

      const result = await clearLocalWorkspace(tokens, "MyWorkspace");

      expect(deleteCount).toBe(1);
      expect(createCount).toBe(1);
      expect(result.id).toBe("ws-456");
      expect(result.name).toBe("MyWorkspace");
      expect(result.token).toBe("new-token");
    });

    it("throws LocalApiError when workspace not found", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/v1/user/workspaces`, () => {
          return HttpResponse.json({
            organization_id: "org-123",
            workspaces: [],
          });
        })
      );

      await expect(clearLocalWorkspace(tokens, "NonExistent")).rejects.toThrow(
        LocalApiError
      );
      await expect(clearLocalWorkspace(tokens, "NonExistent")).rejects.toThrow(
        "Workspace 'NonExistent' not found"
      );
    });
  });
});
