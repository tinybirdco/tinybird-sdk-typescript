import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import {
  getLocalTokens,
  listLocalWorkspaces,
  createLocalWorkspace,
  getOrCreateLocalWorkspace,
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
        http.get(`${LOCAL_BASE_URL}/tokens?from=ts-sdk`, () => {
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
        http.get(`${LOCAL_BASE_URL}/tokens?from=ts-sdk`, () => {
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
        http.get(`${LOCAL_BASE_URL}/tokens?from=ts-sdk`, () => {
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
        http.get(`${LOCAL_BASE_URL}/tokens?from=ts-sdk`, () => {
          return HttpResponse.error();
        })
      );

      await expect(getLocalTokens()).rejects.toThrow(LocalNotRunningError);
    });

    it("throws LocalApiError when response is invalid", async () => {
      server.use(
        http.get(`${LOCAL_BASE_URL}/tokens?from=ts-sdk`, () => {
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
          `${LOCAL_BASE_URL}/v1/user/workspaces?with_organization=true&token=admin-token&from=ts-sdk`,
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
          `${LOCAL_BASE_URL}/v1/user/workspaces?with_organization=true&token=admin-token&from=ts-sdk`,
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
        http.post(`${LOCAL_BASE_URL}/v1/workspaces?from=ts-sdk`, async ({ request }) => {
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
        http.post(`${LOCAL_BASE_URL}/v1/workspaces?from=ts-sdk`, () => {
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
          `${LOCAL_BASE_URL}/v1/user/workspaces?with_organization=true&token=admin-token&from=ts-sdk`,
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
          `${LOCAL_BASE_URL}/v1/user/workspaces?with_organization=true&token=admin-token&from=ts-sdk`,
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
        http.post(`${LOCAL_BASE_URL}/v1/workspaces?from=ts-sdk`, () => {
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
});
