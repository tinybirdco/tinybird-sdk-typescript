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

  describe("schema path detection", () => {
    it("creates lib/tinybird.ts when project has no src folder", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("lib/tinybird.ts");
      expect(fs.existsSync(path.join(tempDir, "lib", "tinybird.ts"))).toBe(true);
    });

    it("creates src/lib/tinybird.ts when project has src folder", async () => {
      // Create src folder to simulate existing project
      fs.mkdirSync(path.join(tempDir, "src"));

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("src/lib/tinybird.ts");
      expect(
        fs.existsSync(path.join(tempDir, "src", "lib", "tinybird.ts"))
      ).toBe(true);
    });

    it("creates tinybird.json with correct include path for lib", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird.json");

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual(["lib/tinybird.ts"]);
    });

    it("creates tinybird.json with correct include path for src/lib", async () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual(["src/lib/tinybird.ts"]);
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
      expect(config.include).toEqual(["lib/tinybird.ts"]);
    });
  });

  describe("schema file creation", () => {
    it("creates starter file with example datasource and pipe", async () => {
      await runInit({ cwd: tempDir, skipLogin: true });

      const starterContent = fs.readFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "utf-8"
      );

      // Check that it contains example definitions (not defineProject)
      expect(starterContent).toContain("defineDatasource");
      expect(starterContent).toContain("definePipe");
      expect(starterContent).toContain("export const pageViews");
      expect(starterContent).toContain("export const topPages");
    });

    it("skips schema file if it already exists", async () => {
      fs.mkdirSync(path.join(tempDir, "lib"), { recursive: true });
      fs.writeFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "// existing content"
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true });

      expect(result.success).toBe(true);
      expect(result.skipped).toContain("lib/tinybird.ts");

      // Verify it wasn't overwritten
      const content = fs.readFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "utf-8"
      );
      expect(content).toBe("// existing content");
    });

    it("overwrites starter file with force option", async () => {
      fs.mkdirSync(path.join(tempDir, "lib"), { recursive: true });
      fs.writeFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "// existing content"
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, force: true });

      expect(result.success).toBe(true);
      expect(result.created).toContain("lib/tinybird.ts");

      const content = fs.readFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "utf-8"
      );
      expect(content).toContain("defineDatasource");
      expect(content).toContain("definePipe");
    });
  });

  describe("directory creation", () => {
    it("creates lib directory if it does not exist", async () => {
      expect(fs.existsSync(path.join(tempDir, "lib"))).toBe(false);

      await runInit({ cwd: tempDir, skipLogin: true });

      expect(fs.existsSync(path.join(tempDir, "lib"))).toBe(true);
    });

    it("creates src/lib directory if project has src folder", async () => {
      fs.mkdirSync(path.join(tempDir, "src"));
      expect(fs.existsSync(path.join(tempDir, "src", "lib"))).toBe(false);

      await runInit({ cwd: tempDir, skipLogin: true });

      expect(fs.existsSync(path.join(tempDir, "src", "lib"))).toBe(true);
    });
  });
});
