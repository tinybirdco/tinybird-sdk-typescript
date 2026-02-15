import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { runInit, findExistingDatafiles } from "./init.js";

// Mock the auth module to avoid browser login
vi.mock("../auth.js", () => ({
  browserLogin: vi.fn().mockResolvedValue({ success: false }),
}));

// Mock the region-selector module to avoid interactive prompts
vi.mock("../region-selector.js", () => ({
  selectRegion: vi.fn().mockResolvedValue({
    success: true,
    apiHost: "https://api.tinybird.co",
  }),
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
    it("creates lib/tinybird.ts when project has no src folder", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("lib/tinybird.ts");
      expect(fs.existsSync(path.join(tempDir, "lib", "tinybird.ts"))).toBe(true);
    });

    it("creates src/lib/tinybird.ts when project has src folder", async () => {
      // Create src folder to simulate existing project
      fs.mkdirSync(path.join(tempDir, "src"));

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "src/lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("src/lib/tinybird.ts");
      expect(
        fs.existsSync(path.join(tempDir, "src", "lib", "tinybird.ts"))
      ).toBe(true);
    });

    it("creates tinybird.config.json with correct include path for lib/tinybird.ts", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird.config.json");

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8")
      );
      expect(config.include).toContain("lib/tinybird.ts");
      expect(config.token).toBe("${TINYBIRD_TOKEN}");
    });

    it("creates tinybird.config.json with correct include path for src/lib/tinybird.ts", async () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "src/lib/tinybird.ts" });

      expect(result.success).toBe(true);

      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).toContain('"src/lib/tinybird.ts"');
    });
  });

  describe("config file creation", () => {
    it("creates tinybird.config.json with default values", async () => {
      await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8")
      );

      expect(config.token).toBe("${TINYBIRD_TOKEN}");
      expect(config.baseUrl).toBe("https://api.tinybird.co");
      expect(config.devMode).toBe("branch");
    });

    it("updates legacy tinybird.json if it already exists", async () => {
      const existingConfig = {
        include: ["custom.ts"],
        token: "existing",
        devMode: "local",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(existingConfig)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird.json (updated)");

      // Verify include/devMode updated but token preserved
      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.json"), "utf-8")
      );
      expect(config.include).toEqual(["lib/tinybird.ts"]);
      expect(config.devMode).toBe("branch");
      expect(config.token).toBe("existing");
    });

    it("updates tinybird.config.json if it already exists", async () => {
      const existingConfig = {
        include: ["custom.ts"],
        token: "existing",
        devMode: "local",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.config.json"),
        JSON.stringify(existingConfig)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("tinybird.config.json (updated)");

      // Verify include/devMode updated but token preserved
      const config = JSON.parse(
        fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8")
      );
      expect(config.include).toEqual(["lib/tinybird.ts"]);
      expect(config.devMode).toBe("branch");
      expect(config.token).toBe("existing");
    });

    it("overwrites existing config with force option", async () => {
      const existingConfig = { include: ["custom.ts"], token: "existing" };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(existingConfig)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, force: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      // With force, it creates a new tinybird.config.json
      expect(result.created).toContain("tinybird.config.json");

      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).toContain('"lib/tinybird.ts"');
    });
  });

  describe("file content creation", () => {
    it("creates tinybird.ts with example datasource, endpoint, and client", async () => {
      await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      const content = fs.readFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "utf-8"
      );

      // Check datasource content
      expect(content).toContain("defineDatasource");
      expect(content).toContain("export const pageViews");
      expect(content).toContain("InferRow");
      expect(content).toContain("PageViewsRow");

      // Check endpoint content
      expect(content).toContain("defineEndpoint");
      expect(content).toContain("export const topPages");
      expect(content).toContain("InferParams");
      expect(content).toContain("InferOutputRow");
      expect(content).toContain("TopPagesParams");
      expect(content).toContain("TopPagesOutput");

      // Check client content
      expect(content).toContain("new Tinybird");
    });

    it("skips tinybird.ts if it already exists", async () => {
      fs.mkdirSync(path.join(tempDir, "lib"), { recursive: true });
      fs.writeFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "// existing content"
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.skipped).toContain("lib/tinybird.ts");

      // Verify it wasn't overwritten
      const content = fs.readFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "utf-8"
      );
      expect(content).toBe("// existing content");
    });

    it("overwrites tinybird.ts with force option", async () => {
      fs.mkdirSync(path.join(tempDir, "lib"), { recursive: true });
      fs.writeFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "// existing content"
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, force: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("lib/tinybird.ts");

      const content = fs.readFileSync(
        path.join(tempDir, "lib", "tinybird.ts"),
        "utf-8"
      );
      expect(content).toContain("defineDatasource");
    });
  });

  describe("package.json scripts", () => {
    it("adds tinybird scripts to existing package.json", async () => {
      const packageJson = { name: "test-project", scripts: { dev: "next dev" } };
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify(packageJson, null, 2)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).toContain("package.json (added tinybird scripts)");

      const updatedPackageJson = JSON.parse(
        fs.readFileSync(path.join(tempDir, "package.json"), "utf-8")
      );
      expect(updatedPackageJson.scripts["tinybird:dev"]).toBe("tinybird dev");
      expect(updatedPackageJson.scripts["tinybird:build"]).toBe("tinybird build");
      expect(updatedPackageJson.scripts["tinybird:deploy"]).toBe("tinybird deploy");
      expect(updatedPackageJson.scripts["tinybird:preview"]).toBe("tinybird preview");
      expect(updatedPackageJson.scripts.dev).toBe("next dev"); // preserved
    });

    it("does not overwrite existing tinybird scripts", async () => {
      const packageJson = {
        name: "test-project",
        scripts: {
          "tinybird:dev": "custom dev command",
          "tinybird:build": "custom build command",
          "tinybird:deploy": "custom deploy command",
          "tinybird:preview": "custom preview command",
        },
      };
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify(packageJson, null, 2)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).not.toContain("package.json (added tinybird scripts)");

      const updatedPackageJson = JSON.parse(
        fs.readFileSync(path.join(tempDir, "package.json"), "utf-8")
      );
      expect(updatedPackageJson.scripts["tinybird:dev"]).toBe("custom dev command");
      expect(updatedPackageJson.scripts["tinybird:build"]).toBe("custom build command");
      expect(updatedPackageJson.scripts["tinybird:deploy"]).toBe("custom deploy command");
      expect(updatedPackageJson.scripts["tinybird:preview"]).toBe("custom preview command");
    });

    it("creates scripts object if package.json has no scripts", async () => {
      const packageJson = { name: "test-project" };
      fs.writeFileSync(
        path.join(tempDir, "package.json"),
        JSON.stringify(packageJson, null, 2)
      );

      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);

      const updatedPackageJson = JSON.parse(
        fs.readFileSync(path.join(tempDir, "package.json"), "utf-8")
      );
      expect(updatedPackageJson.scripts["tinybird:dev"]).toBe("tinybird dev");
      expect(updatedPackageJson.scripts["tinybird:build"]).toBe("tinybird build");
      expect(updatedPackageJson.scripts["tinybird:preview"]).toBe("tinybird preview");
    });

    it("does not fail if no package.json exists", async () => {
      const result = await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(result.success).toBe(true);
      expect(result.created).not.toContain("package.json (added tinybird scripts)");
    });
  });

  describe("directory creation", () => {
    it("creates lib directory if it does not exist", async () => {
      expect(fs.existsSync(path.join(tempDir, "lib"))).toBe(false);

      await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "lib/tinybird.ts" });

      expect(fs.existsSync(path.join(tempDir, "lib"))).toBe(true);
    });

    it("creates src/lib directory if project has src folder", async () => {
      fs.mkdirSync(path.join(tempDir, "src"));
      expect(fs.existsSync(path.join(tempDir, "src", "lib"))).toBe(false);

      await runInit({ cwd: tempDir, skipLogin: true, devMode: "branch", clientPath: "src/lib/tinybird.ts" });

      expect(fs.existsSync(path.join(tempDir, "src", "lib"))).toBe(true);
    });
  });

  describe("findExistingDatafiles", () => {
    it("finds .datasource files in the project", () => {
      fs.mkdirSync(path.join(tempDir, "datasources"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "datasources", "events.datasource"), "");
      fs.writeFileSync(path.join(tempDir, "datasources", "users.datasource"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).toContain("datasources/events.datasource");
      expect(files).toContain("datasources/users.datasource");
    });

    it("finds .pipe files in the project", () => {
      fs.mkdirSync(path.join(tempDir, "pipes"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "pipes", "top_events.pipe"), "");
      fs.writeFileSync(path.join(tempDir, "pipes", "analytics.pipe"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).toContain("pipes/analytics.pipe");
      expect(files).toContain("pipes/top_events.pipe");
    });

    it("finds both .datasource and .pipe files", () => {
      fs.mkdirSync(path.join(tempDir, "tinybird"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "tinybird", "events.datasource"), "");
      fs.writeFileSync(path.join(tempDir, "tinybird", "top_events.pipe"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).toHaveLength(2);
      expect(files).toContain("tinybird/events.datasource");
      expect(files).toContain("tinybird/top_events.pipe");
    });

    it("skips node_modules directory", () => {
      fs.mkdirSync(path.join(tempDir, "node_modules", "some-package"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "node_modules", "some-package", "test.datasource"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).not.toContain("node_modules/some-package/test.datasource");
      expect(files).toHaveLength(0);
    });

    it("skips hidden directories", () => {
      fs.mkdirSync(path.join(tempDir, ".git", "hooks"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, ".git", "hooks", "test.datasource"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).not.toContain(".git/hooks/test.datasource");
      expect(files).toHaveLength(0);
    });

    it("skips dist and build directories", () => {
      fs.mkdirSync(path.join(tempDir, "dist"), { recursive: true });
      fs.mkdirSync(path.join(tempDir, "build"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "dist", "test.datasource"), "");
      fs.writeFileSync(path.join(tempDir, "build", "test.pipe"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).toHaveLength(0);
    });

    it("returns empty array when no datafiles exist", () => {
      const files = findExistingDatafiles(tempDir);

      expect(files).toEqual([]);
    });

    it("respects maxDepth parameter", () => {
      // Create a deeply nested file
      fs.mkdirSync(path.join(tempDir, "a", "b", "c", "d", "e", "f"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "a", "b", "c", "d", "e", "f", "deep.datasource"), "");
      // Create a shallow file
      fs.writeFileSync(path.join(tempDir, "a", "shallow.datasource"), "");

      const filesDefault = findExistingDatafiles(tempDir, 5);
      expect(filesDefault).toContain("a/shallow.datasource");
      expect(filesDefault).not.toContain("a/b/c/d/e/f/deep.datasource");

      const filesDeep = findExistingDatafiles(tempDir, 10);
      expect(filesDeep).toContain("a/b/c/d/e/f/deep.datasource");
    });

    it("returns files in sorted order", () => {
      fs.mkdirSync(path.join(tempDir, "datasources"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "datasources", "zebra.datasource"), "");
      fs.writeFileSync(path.join(tempDir, "datasources", "alpha.datasource"), "");
      fs.writeFileSync(path.join(tempDir, "datasources", "beta.datasource"), "");

      const files = findExistingDatafiles(tempDir);

      expect(files).toEqual([
        "datasources/alpha.datasource",
        "datasources/beta.datasource",
        "datasources/zebra.datasource",
      ]);
    });
  });

  describe("existing datafiles detection", () => {
    it("includes existing datafiles in config when user opts in", async () => {
      // Create existing datafiles
      fs.mkdirSync(path.join(tempDir, "datasources"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "datasources", "events.datasource"), "");
      fs.mkdirSync(path.join(tempDir, "pipes"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "pipes", "top_events.pipe"), "");

      const result = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
        skipDatafilePrompt: true,
        includeExistingDatafiles: true,
      });

      expect(result.success).toBe(true);
      expect(result.existingDatafiles).toContain("datasources/events.datasource");
      expect(result.existingDatafiles).toContain("pipes/top_events.pipe");

      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).toContain('"datasources/events.datasource"');
      expect(content).toContain('"pipes/top_events.pipe"');
    });

    it("does not include existing datafiles when user opts out", async () => {
      // Create existing datafiles
      fs.mkdirSync(path.join(tempDir, "datasources"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "datasources", "events.datasource"), "");

      const result = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
        skipDatafilePrompt: true,
        includeExistingDatafiles: false,
      });

      expect(result.success).toBe(true);
      expect(result.existingDatafiles).toBeUndefined();

      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).not.toContain("datasources/events.datasource");
      expect(content).toContain('"lib/tinybird.ts"');
    });

    it("preserves TypeScript include paths alongside datafiles", async () => {
      // Create existing datafiles
      fs.mkdirSync(path.join(tempDir, "datasources"), { recursive: true });
      fs.writeFileSync(path.join(tempDir, "datasources", "events.datasource"), "");

      const result = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
        skipDatafilePrompt: true,
        includeExistingDatafiles: true,
      });

      expect(result.success).toBe(true);

      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      // Should have both TypeScript file AND datafiles
      expect(content).toContain('"lib/tinybird.ts"');
      expect(content).toContain('"datasources/events.datasource"');
    });

    it("handles projects with no existing datafiles", async () => {
      const result = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
        skipDatafilePrompt: true,
        includeExistingDatafiles: true,
      });

      expect(result.success).toBe(true);
      expect(result.existingDatafiles).toBeUndefined();

      const content = fs.readFileSync(path.join(tempDir, "tinybird.config.json"), "utf-8");
      expect(content).toContain('"lib/tinybird.ts"');
    });
  });

  describe("tool installation selection", () => {
    it("returns selected tools and skips installation when requested", async () => {
      const result = await runInit({
        cwd: tempDir,
        skipLogin: true,
        devMode: "branch",
        clientPath: "lib/tinybird.ts",
        installTools: ["skills", "syntax-highlighting"],
        skipToolsInstall: true,
      });

      expect(result.success).toBe(true);
      expect(result.installTools).toEqual(["skills", "syntax-highlighting"]);
      expect(result.installedTools).toBeUndefined();
    });
  });
});
