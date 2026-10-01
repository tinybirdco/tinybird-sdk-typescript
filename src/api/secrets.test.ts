import { describe, it, expect, vi, beforeEach } from "vitest";
import {
  createSecret,
  listSecrets,
  removeSecret,
  SecretApiError,
  setSecret,
  updateSecret,
  type SecretApiConfig,
} from "./secrets.js";

// Mock fetch globally
const mockFetch = vi.fn();
global.fetch = mockFetch;

function expectFromParam(url: string) {
  const parsed = new URL(url);
  expect(parsed.searchParams.get("from")).toBe("ts-sdk");
  return parsed;
}

function jsonResponse(body: unknown, init?: { ok?: boolean; status?: number }) {
  return {
    ok: init?.ok ?? true,
    status: init?.status ?? 200,
    json: () => Promise.resolve(body),
    text: () => Promise.resolve(JSON.stringify(body)),
  };
}

function textResponse(body: string, init?: { ok?: boolean; status?: number }) {
  return {
    ok: init?.ok ?? true,
    status: init?.status ?? 200,
    text: () => Promise.resolve(body),
  };
}

describe("Secret API client", () => {
  const config: SecretApiConfig = {
    baseUrl: "https://api.tinybird.co",
    token: "p.admin-token",
  };

  beforeEach(() => {
    mockFetch.mockReset();
  });

  describe("listSecrets", () => {
    it("lists secrets without exposing values", async () => {
      mockFetch.mockResolvedValueOnce(
        jsonResponse({
          variables: [
            { name: "API_KEY", created_at: "2024-01-01", updated_at: "2024-01-02" },
            { name: "DB_PASSWORD", created_at: "2024-01-03", updated_at: "2024-01-03" },
          ],
        })
      );

      const result = await listSecrets(config);

      expect(result).toEqual([
        { name: "API_KEY", created_at: "2024-01-01", updated_at: "2024-01-02" },
        { name: "DB_PASSWORD", created_at: "2024-01-03", updated_at: "2024-01-03" },
      ]);

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/variables");
      expect(init.method).toBe("GET");
      const headers = new Headers(init.headers);
      expect(headers.get("Authorization")).toBe("Bearer p.admin-token");
    });

    it("returns an empty array when the API returns no variables field", async () => {
      mockFetch.mockResolvedValueOnce(jsonResponse({}));

      const result = await listSecrets(config);
      expect(result).toEqual([]);
    });

    it("throws SecretApiError on failure", async () => {
      mockFetch.mockResolvedValueOnce(
        textResponse('{"error": "Forbidden"}', { ok: false, status: 403 })
      );

      await expect(listSecrets(config)).rejects.toThrow(SecretApiError);

      mockFetch.mockResolvedValueOnce(
        textResponse('{"error": "Forbidden"}', { ok: false, status: 403 })
      );

      try {
        await listSecrets(config);
        expect.unreachable();
      } catch (error) {
        expect((error as SecretApiError).status).toBe(403);
        expect((error as SecretApiError).message).toBe("Forbidden");
      }
    });
  });

  describe("createSecret", () => {
    it("creates a secret with name and value as form body", async () => {
      mockFetch.mockResolvedValueOnce(textResponse(""));

      await createSecret(config, "API_KEY", "sk_live_12345");

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/variables");
      expect(init.method).toBe("POST");
      const headers = new Headers(init.headers);
      expect(headers.get("Content-Type")).toBe("application/x-www-form-urlencoded");

      const body = new URLSearchParams(init.body as string);
      expect(body.get("name")).toBe("API_KEY");
      expect(body.get("value")).toBe("sk_live_12345");
    });

    it("rejects an empty secret name", async () => {
      await expect(createSecret(config, "", "value")).rejects.toThrow(
        "Secret name must be a non-empty string."
      );
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it("redacts the secret value from thrown error messages", async () => {
      mockFetch.mockResolvedValueOnce(
        textResponse('{"error": "Invalid value sk_live_12345 for secret"}', {
          ok: false,
          status: 400,
        })
      );

      try {
        await createSecret(config, "API_KEY", "sk_live_12345");
        expect.unreachable();
      } catch (error) {
        expect(error).toBeInstanceOf(SecretApiError);
        const apiError = error as SecretApiError;
        expect(apiError.message).not.toContain("sk_live_12345");
        expect(apiError.message).toContain("[REDACTED]");
        expect(apiError.body).not.toContain("sk_live_12345");
      }
    });
  });

  describe("updateSecret", () => {
    it("updates a secret with only the value as form body", async () => {
      mockFetch.mockResolvedValueOnce(textResponse(""));

      await updateSecret(config, "API_KEY", "sk_live_updated");

      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/variables/API_KEY");
      expect(init.method).toBe("PUT");
      const headers = new Headers(init.headers);
      expect(headers.get("Content-Type")).toBe("application/x-www-form-urlencoded");

      const body = new URLSearchParams(init.body as string);
      expect(body.get("value")).toBe("sk_live_updated");
      expect(body.get("name")).toBeNull();
    });
  });

  describe("setSecret", () => {
    it("creates the secret when it does not already exist", async () => {
      // existence check (GET) -> 404
      mockFetch.mockResolvedValueOnce(textResponse("Not found", { ok: false, status: 404 }));
      // create (POST)
      mockFetch.mockResolvedValueOnce(textResponse(""));

      const result = await setSecret(config, "NEW_SECRET", "value1");

      expect(result).toEqual({ name: "NEW_SECRET", created: true });
      expect(mockFetch).toHaveBeenCalledTimes(2);

      const [existsUrl, existsInit] = mockFetch.mock.calls[0];
      expect(new URL(existsUrl).pathname).toBe("/v0/variables/NEW_SECRET");
      expect(existsInit.method).toBe("GET");

      const [createUrl, createInit] = mockFetch.mock.calls[1];
      expect(new URL(createUrl).pathname).toBe("/v0/variables");
      expect(createInit.method).toBe("POST");
    });

    it("updates the secret when it already exists", async () => {
      // existence check (GET) -> 200
      mockFetch.mockResolvedValueOnce(textResponse('{"name":"EXISTING"}', { ok: true }));
      // update (PUT)
      mockFetch.mockResolvedValueOnce(textResponse(""));

      const result = await setSecret(config, "EXISTING", "value2");

      expect(result).toEqual({ name: "EXISTING", created: false });
      expect(mockFetch).toHaveBeenCalledTimes(2);

      const [, updateInit] = mockFetch.mock.calls[1];
      expect(updateInit.method).toBe("PUT");
    });

    it("treats an existence-check failure (e.g. network error) as not existing, like the CLI", async () => {
      mockFetch.mockRejectedValueOnce(new Error("network error"));
      mockFetch.mockResolvedValueOnce(textResponse(""));

      const result = await setSecret(config, "FLAKY_CHECK", "value3");

      expect(result).toEqual({ name: "FLAKY_CHECK", created: true });
      const [, createInit] = mockFetch.mock.calls[1];
      expect(createInit.method).toBe("POST");
    });

    it("rejects an empty secret name before making any request", async () => {
      await expect(setSecret(config, "", "value")).rejects.toThrow(
        "Secret name must be a non-empty string."
      );
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });

  describe("removeSecret", () => {
    it("deletes a secret", async () => {
      mockFetch.mockResolvedValueOnce(textResponse(""));

      const result = await removeSecret(config, "API_KEY");

      expect(result).toEqual({ name: "API_KEY" });
      const [url, init] = mockFetch.mock.calls[0];
      const parsed = expectFromParam(url);
      expect(parsed.pathname).toBe("/v0/variables/API_KEY");
      expect(init.method).toBe("DELETE");
    });

    it("throws SecretApiError on failure", async () => {
      mockFetch.mockResolvedValueOnce(
        textResponse('{"error": "Secret not found"}', { ok: false, status: 404 })
      );

      await expect(removeSecret(config, "MISSING")).rejects.toThrow(SecretApiError);
    });

    it("rejects an empty secret name", async () => {
      await expect(removeSecret(config, "")).rejects.toThrow(
        "Secret name must be a non-empty string."
      );
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });
});
