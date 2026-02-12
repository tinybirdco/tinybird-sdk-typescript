/**
 * E2E tests for datasources.append functionality
 *
 * Tests the append method for importing data from URLs or local files
 * into Tinybird datasources.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { server } from "./setup.js";
import { http, HttpResponse } from "msw";
import { BASE_URL } from "./handlers.js";

describe("E2E: datasources.append", () => {
  describe("append from URL", () => {
    it("appends data from URL with correct API parameters", async () => {
      let appendCalled = false;
      let capturedDatasourceName: string | null = null;
      let capturedMode: string | null = null;
      let capturedFormat: string | null = null;
      let capturedUrl: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          appendCalled = true;
          const url = new URL(request.url);
          capturedDatasourceName = url.searchParams.get("name");
          capturedMode = url.searchParams.get("mode");
          capturedFormat = url.searchParams.get("format");
          capturedUrl = (await request.text()).replace("url=", "");

          return HttpResponse.json({
            successful_rows: 100,
            quarantined_rows: 0,
            import_id: "import_123",
          });
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      const result = await client.datasources.append("events", {
        url: "https://example.com/data.csv",
      });

      expect(appendCalled).toBe(true);
      expect(capturedDatasourceName).toBe("events");
      expect(capturedMode).toBe("append");
      expect(capturedFormat).toBe("csv");
      expect(decodeURIComponent(capturedUrl!)).toBe("https://example.com/data.csv");
      expect(result.successful_rows).toBe(100);
      expect(result.quarantined_rows).toBe(0);
      expect(result.import_id).toBe("import_123");
    });

    it("appends data with CSV dialect options", async () => {
      let capturedDelimiter: string | null = null;
      let capturedNewLine: string | null = null;
      let capturedEscapeChar: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          const url = new URL(request.url);
          capturedDelimiter = url.searchParams.get("dialect_delimiter");
          capturedNewLine = url.searchParams.get("dialect_new_line");
          capturedEscapeChar = url.searchParams.get("dialect_escapechar");

          return HttpResponse.json({
            successful_rows: 50,
            quarantined_rows: 0,
          });
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      await client.datasources.append("events", {
        url: "https://example.com/data.csv",
        csvDialect: {
          delimiter: ";",
          newLine: "\r\n",
          escapeChar: "\\",
        },
      });

      expect(capturedDelimiter).toBe(";");
      expect(capturedNewLine).toBe("\r\n");
      expect(capturedEscapeChar).toBe("\\");
    });
  });

  describe("format auto-detection", () => {
    it("auto-detects csv format from URL", async () => {
      let capturedFormat: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          const url = new URL(request.url);
          capturedFormat = url.searchParams.get("format");
          return HttpResponse.json({ successful_rows: 1, quarantined_rows: 0 });
        })
      );

      const { createClient } = await import("../src/client/base.js");
      const client = createClient({ baseUrl: BASE_URL, token: "p.test-token" });

      await client.datasources.append("events", {
        url: "https://example.com/data.csv",
      });

      expect(capturedFormat).toBe("csv");
    });

    it("auto-detects ndjson format from URL", async () => {
      let capturedFormat: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          const url = new URL(request.url);
          capturedFormat = url.searchParams.get("format");
          return HttpResponse.json({ successful_rows: 1, quarantined_rows: 0 });
        })
      );

      const { createClient } = await import("../src/client/base.js");
      const client = createClient({ baseUrl: BASE_URL, token: "p.test-token" });

      await client.datasources.append("events", {
        url: "https://example.com/events.ndjson",
      });

      expect(capturedFormat).toBe("ndjson");
    });

    it("auto-detects jsonl as ndjson format", async () => {
      let capturedFormat: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          const url = new URL(request.url);
          capturedFormat = url.searchParams.get("format");
          return HttpResponse.json({ successful_rows: 1, quarantined_rows: 0 });
        })
      );

      const { createClient } = await import("../src/client/base.js");
      const client = createClient({ baseUrl: BASE_URL, token: "p.test-token" });

      await client.datasources.append("events", {
        url: "https://example.com/events.jsonl",
      });

      expect(capturedFormat).toBe("ndjson");
    });

    it("auto-detects parquet format from URL", async () => {
      let capturedFormat: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          const url = new URL(request.url);
          capturedFormat = url.searchParams.get("format");
          return HttpResponse.json({ successful_rows: 1, quarantined_rows: 0 });
        })
      );

      const { createClient } = await import("../src/client/base.js");
      const client = createClient({ baseUrl: BASE_URL, token: "p.test-token" });

      await client.datasources.append("events", {
        url: "https://example.com/data.parquet",
      });

      expect(capturedFormat).toBe("parquet");
    });

    it("strips query string when detecting format", async () => {
      let capturedFormat: string | null = null;

      server.use(
        http.post(`${BASE_URL}/v0/datasources`, async ({ request }) => {
          const url = new URL(request.url);
          capturedFormat = url.searchParams.get("format");
          return HttpResponse.json({ successful_rows: 1, quarantined_rows: 0 });
        })
      );

      const { createClient } = await import("../src/client/base.js");
      const client = createClient({ baseUrl: BASE_URL, token: "p.test-token" });

      await client.datasources.append("events", {
        url: "https://example.com/data.csv?token=abc&version=2",
      });

      expect(capturedFormat).toBe("csv");
    });
  });

  describe("error handling", () => {
    it("handles append API errors", async () => {
      server.use(
        http.post(`${BASE_URL}/v0/datasources`, () => {
          return HttpResponse.json(
            { error: "Datasource not found" },
            { status: 404 }
          );
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      await expect(
        client.datasources.append("nonexistent", {
          url: "https://example.com/data.csv",
        })
      ).rejects.toThrow("Datasource not found");
    });

    it("handles rate limit errors", async () => {
      server.use(
        http.post(`${BASE_URL}/v0/datasources`, () => {
          return HttpResponse.json(
            { error: "Rate limit exceeded" },
            { status: 429 }
          );
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      await expect(
        client.datasources.append("events", {
          url: "https://example.com/data.csv",
        })
      ).rejects.toThrow("Rate limit exceeded");
    });

    it("handles server errors", async () => {
      server.use(
        http.post(`${BASE_URL}/v0/datasources`, () => {
          return HttpResponse.json(
            { error: "Internal server error" },
            { status: 500 }
          );
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      await expect(
        client.datasources.append("events", {
          url: "https://example.com/data.csv",
        })
      ).rejects.toThrow("Internal server error");
    });
  });

  describe("validation", () => {
    it("throws error when neither url nor file is provided", async () => {
      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      await expect(
        client.datasources.append("events", {})
      ).rejects.toThrow("Either 'url' or 'file' must be provided in options");
    });

    it("throws error when both url and file are provided", async () => {
      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      await expect(
        client.datasources.append("events", {
          url: "https://example.com/data.csv",
          file: "./data.csv",
        })
      ).rejects.toThrow("Only one of 'url' or 'file' can be provided, not both");
    });
  });

  describe("append result", () => {
    it("returns successful_rows and quarantined_rows", async () => {
      server.use(
        http.post(`${BASE_URL}/v0/datasources`, () => {
          return HttpResponse.json({
            successful_rows: 950,
            quarantined_rows: 50,
          });
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      const result = await client.datasources.append("events", {
        url: "https://example.com/data.csv",
      });

      expect(result.successful_rows).toBe(950);
      expect(result.quarantined_rows).toBe(50);
    });

    it("returns import_id when provided", async () => {
      server.use(
        http.post(`${BASE_URL}/v0/datasources`, () => {
          return HttpResponse.json({
            successful_rows: 100,
            quarantined_rows: 0,
            import_id: "import_abc123",
          });
        })
      );

      const { createClient } = await import("../src/client/base.js");

      const client = createClient({
        baseUrl: BASE_URL,
        token: "p.test-token",
      });

      const result = await client.datasources.append("events", {
        url: "https://example.com/data.csv",
      });

      expect(result.import_id).toBe("import_abc123");
    });
  });
});
