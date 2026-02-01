import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { runInit } from "./init.js";

// Mock the auth module to avoid browser login
vi.mock("../auth.js", () => ({
  browserLogin: vi.fn().mockResolvedValue({ success: false }),
}));

describe("Init Command", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-init-test-"));
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
  });

  describe("folder structure creation", () => {
    it("creates tinybird folder with datasources.ts, pipes.ts, client.ts when project has no src folder", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird/datasources.ts");
      expect(result.created).toContain("tinybird/pipes.ts");
      expect(result.created).toContain("tinybird/client.ts");
      expect(fs.existsSync(path.join(tempDir, "tinybird", "datasources.ts"))).toBe(true);
      expect(fs.existsSync(path.join(tempDir, "tinybird", "pipes.ts"))).toBe(true);
      expect(fs.existsSync(path.join(tempDir, "tinybird", "client.ts"))).toBe(true);
    });

    it("creates src/tinybird folder with files when project has src folder", async () => {
      // Create src folder to simulate existing project
      fs.mkdirSync(path.join(tempDir, "src"));

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("src/tinybird/datasources.ts");
      expect(result.created).toContain("src/tinybird/pipes.ts");
      expect(result.created).toContain("src/tinybird/client.ts");
      expect(
        fs.existsSync(path.join(tempDir, "src", "tinybird", "datasources.ts"))
      ).toBe(true);
      expect(
        fs.existsSync(path.join(tempDir, "src", "tinybird", "pipes.ts"))
      ).toBe(true);
      expect(
        fs.existsSync(path.join(tempDir, "src", "tinybird", "client.ts"))
      ).toBe(true);
    });

    it("creates tinybird.json with correct include paths for tinybird folder", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird.json");

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual([
        "tinybird/datasources.ts",
        "tinybird/pipes.ts",
      ]);
    });

    it("creates tinybird.json with correct include paths for src/tinybird", async () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual([
        "src/tinybird/datasources.ts",
        "src/tinybird/pipes.ts",
      ]);
    });
  });

  describe("config file creation", () => {
    it("creates tinybird.json with default values", async () => {
      await runInit({ cwd: tempDir, skipLogin: true });

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );

      expect(config.token).toBe("${TINYBIRD_TOKEN}");
      expect(config.baseUrl).toBe("https://api.tinybird.co");
    });

    it("skips tinybird.json if it already exists", async () => {
      const existingConfig = { schema: "custom.ts", token: "existing" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(existingConfig)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.skipped).toContain("tinybird.json");

      // Verify it wasn't overwritten
      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.schema).toBe("custom.ts");
    });

    it("overwrites tinybird.json with force option", async () => {
      const existingConfig = { include: ["custom.ts"], token: "existing" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(existingConfig)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, force: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird.json");

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual([
        "tinybird/datasources.ts",
        "tinybird/pipes.ts",
      ]);
    });
  });

  describe("file content creation", () => {
    it("creates datasources.ts with example datasource and InferRow type", async () => {
      await runInit({ cwd: tempDir, skipLogin: true });

      const content = fs.readFileSync(
        path.join(tempDir, "tinybird", "datasources.ts"),
        "utf-8"
      );

      expect(content).toContain("defineDatasource");
      expect(content).toContain("export const pageViews");
      expect(content).toContain("InferRow");
      expect(content).toContain("PageViewsRow");
    });

    it("creates pipes.ts with example endpoint and types", async () => {
      await runInit({ cwd: tempDir, skipLogin: true });

      const content = fs.readFileSync(
        path.join(tempDir, "tinybird", "pipes.ts"),
        "utf-8"
      );

      expect(content).toContain("defineEndpoint");
      expect(content).toContain("export const topPages");
      expect(content).toContain("InferParams");
      expect(content).toContain("InferOutputRow");
      expect(content).toContain("TopPagesParams");
      expect(content).toContain("TopPagesOutput");
    });

    it("creates client.ts with createTinybirdClient", async () => {
      await runInit({ cwd: tempDir, skipLogin: true });

      const content = fs.readFileSync(
        path.join(tempDir, "tinybird", "client.ts"),
        "utf-8"
      );

      expect(content).toContain("createTinybirdClient");
      expect(content).toContain("export const tinybird");
      expect(content).toContain("pageViews");
      expect(content).toContain("topPages");
    });

    it("skips files that already exist", async () => {
      fs.mkdirSync(path.join(tempDir, "tinybird"), { recursive: true });
      fs.writeFileSync(
        path.join(tempDir, "tinybird", "datasources.ts"),
        "// existing content"
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.skipped).toContain("tinybird/datasources.ts");

      // Verify it wasn't overwritten
      const content = fs.readFileSync(
        path.join(tempDir, "tinybird", "datasources.ts"),
        "utf-8"
      );
      expect(content).toBe("// existing content");
    });

    it("overwrites files with force option", async () => {
      fs.mkdirSync(path.join(tempDir, "tinybird"), { recursive: true });
      fs.writeFileSync(
        path.join(tempDir, "tinybird", "datasources.ts"),
        "// existing content"
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, force: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird/datasources.ts");

      const content = fs.readFileSync(
        path.join(tempDir, "tinybird", "datasources.ts"),
        "utf-8"
      );
      expect(content).toContain("defineDatasource");
    });
  });

  describe("directory creation", () => {
    it("creates tinybird directory if it does not exist", async () => {
      expect(fs.existsSync(path.join(tempDir, "tinybird"))).toBe(false);

      await runInit({ cwd: tempDir, skipLogin: true });

      expect(fs.existsSync(path.join(tempDir, "tinybird"))).toBe(true);
    });

    it("creates src/tinybird directory if project has src folder", async () => {
      fs.mkdirSync(path.join(tempDir, "src"));
      expect(fs.existsSync(path.join(tempDir, "src", "tinybird"))).toBe(false);

      await runInit({ cwd: tempDir, skipLogin: true });

      expect(fs.existsSync(path.join(tempDir, "src", "tinybird"))).toBe(true);
    });
  });
});
