import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import {
  hasSrcFolder,
  getLibDir,
  getRelativeLibDir,
  getTinybirdSchemaPath,
  getRelativeSchemaPath,
  findConfigFile,
  loadConfig,
  getConfigPath,
  configExists,
  updateConfig,
  hasValidToken,
} from "./config.js";

describe("Config", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "tinybird-config-test-"));
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true });
    } catch {
      // Ignore cleanup errors
    }
    vi.restoreAllMocks();
  });

  describe("hasSrcFolder", () => {
    it("returns true when src folder exists", () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      expect(hasSrcFolder(tempDir)).toBe(true);
    });

    it("returns false when src folder does not exist", () => {
      expect(hasSrcFolder(tempDir)).toBe(false);
    });

    it("returns false when src is a file not a folder", () => {
      fs.writeFileSync(path.join(tempDir, "src"), "not a folder");

      expect(hasSrcFolder(tempDir)).toBe(false);
    });
  });

  describe("getLibDir", () => {
    it("returns src/tinybird when project has src folder", () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      expect(getLibDir(tempDir)).toBe(path.join(tempDir, "src", "tinybird"));
    });

    it("returns tinybird when project does not have src folder", () => {
      expect(getLibDir(tempDir)).toBe(path.join(tempDir, "tinybird"));
    });
  });

  describe("getRelativeLibDir", () => {
    it("returns src/tinybird when project has src folder", () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      expect(getRelativeLibDir(tempDir)).toBe("src/tinybird");
    });

    it("returns tinybird when project does not have src folder", () => {
      expect(getRelativeLibDir(tempDir)).toBe("tinybird");
    });
  });

  describe("getTinybirdSchemaPath", () => {
    it("returns src/tinybird/datasources.ts when project has src folder", () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      expect(getTinybirdSchemaPath(tempDir)).toBe(
        path.join(tempDir, "src", "tinybird", "datasources.ts")
      );
    });

    it("returns tinybird/datasources.ts when project does not have src folder", () => {
      expect(getTinybirdSchemaPath(tempDir)).toBe(
        path.join(tempDir, "tinybird", "datasources.ts")
      );
    });
  });

  describe("getRelativeSchemaPath", () => {
    it("returns src/tinybird/datasources.ts when project has src folder", () => {
      fs.mkdirSync(path.join(tempDir, "src"));

      expect(getRelativeSchemaPath(tempDir)).toBe("src/tinybird/datasources.ts");
    });

    it("returns tinybird/datasources.ts when project does not have src folder", () => {
      expect(getRelativeSchemaPath(tempDir)).toBe("tinybird/datasources.ts");
    });
  });

  describe("findConfigFile", () => {
    it("finds tinybird.json in current directory", () => {
      const configPath = path.join(tempDir, "tinybird.json");
      fs.writeFileSync(configPath, "{}");

      expect(findConfigFile(tempDir)).toBe(configPath);
    });

    it("finds tinybird.json in parent directory", () => {
      const nestedDir = path.join(tempDir, "src", "app");
      fs.mkdirSync(nestedDir, { recursive: true });
      const configPath = path.join(tempDir, "tinybird.json");
      fs.writeFileSync(configPath, "{}");

      expect(findConfigFile(nestedDir)).toBe(configPath);
    });

    it("returns null when no config file exists", () => {
      expect(findConfigFile(tempDir)).toBe(null);
    });
  });

  describe("getConfigPath", () => {
    it("returns path to tinybird.json in directory", () => {
      expect(getConfigPath(tempDir)).toBe(path.join(tempDir, "tinybird.json"));
    });
  });

  describe("configExists", () => {
    it("returns true when config exists", () => {
      fs.writeFileSync(path.join(tempDir, "tinybird.json"), "{}");

      expect(configExists(tempDir)).toBe(true);
    });

    it("returns false when config does not exist", () => {
      expect(configExists(tempDir)).toBe(false);
    });
  });

  describe("loadConfig", () => {
    beforeEach(() => {
      // Mock git functions to avoid git dependency in tests
      vi.mock("./git.js", () => ({
        getCurrentGitBranch: () => "main",
        isMainBranch: () => true,
        getTinybirdBranchName: () => null,
      }));
    });

    it("throws error when no config file exists", () => {
      expect(() => loadConfig(tempDir)).toThrow("Could not find tinybird.json");
    });

    it("loads config with include array", () => {
      const config = {
        include: ["lib/datasources.ts", "lib/pipes.ts"],
        token: "test-token",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      expect(result.include).toEqual(["lib/datasources.ts", "lib/pipes.ts"]);
      expect(result.token).toBe("test-token");
      expect(result.baseUrl).toBe("https://api.tinybird.co");
    });

    it("loads legacy config with schema (backward compat)", () => {
      const config = {
        schema: "src/lib/tinybird.ts",
        token: "test-token",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      // Legacy schema is converted to include array
      expect(result.include).toEqual(["src/lib/tinybird.ts"]);
    });

    it("loads config with custom baseUrl", () => {
      const config = {
        schema: "lib/tinybird.ts",
        token: "test-token",
        baseUrl: "https://api.us-east.tinybird.co",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      expect(result.baseUrl).toBe("https://api.us-east.tinybird.co");
    });

    it("interpolates environment variables in token", () => {
      process.env.TEST_TINYBIRD_TOKEN = "secret-token-from-env";
      const config = {
        schema: "lib/tinybird.ts",
        token: "${TEST_TINYBIRD_TOKEN}",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      expect(result.token).toBe("secret-token-from-env");
      delete process.env.TEST_TINYBIRD_TOKEN;
    });

    it("throws error when env var is not set", () => {
      delete process.env.NONEXISTENT_VAR;
      const config = {
        schema: "lib/tinybird.ts",
        token: "${NONEXISTENT_VAR}",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(() => loadConfig(tempDir)).toThrow(
        "Environment variable NONEXISTENT_VAR is not set"
      );
    });

    it("throws error when include field is missing", () => {
      const config = {
        token: "test-token",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(() => loadConfig(tempDir)).toThrow("Missing 'include' field");
    });

    it("throws error when token field is missing", () => {
      const config = {
        schema: "lib/tinybird.ts",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(() => loadConfig(tempDir)).toThrow("Missing 'token' field");
    });

    it("throws error for invalid JSON", () => {
      fs.writeFileSync(path.join(tempDir, "tinybird.json"), "not valid json");

      expect(() => loadConfig(tempDir)).toThrow("Failed to parse");
    });

    it("defaults devMode to branch when not specified", () => {
      const config = {
        include: ["lib/datasources.ts"],
        token: "test-token",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      expect(result.devMode).toBe("branch");
    });

    it("loads devMode as branch when explicitly set", () => {
      const config = {
        include: ["lib/datasources.ts"],
        token: "test-token",
        devMode: "branch",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      expect(result.devMode).toBe("branch");
    });

    it("loads devMode as local when set", () => {
      const config = {
        include: ["lib/datasources.ts"],
        token: "test-token",
        devMode: "local",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      const result = loadConfig(tempDir);

      expect(result.devMode).toBe("local");
    });
  });

  describe("updateConfig", () => {
    it("updates existing config file", () => {
      const configPath = path.join(tempDir, "tinybird.json");
      const initialConfig = {
        schema: "lib/tinybird.ts",
        token: "${TINYBIRD_TOKEN}",
        baseUrl: "https://api.tinybird.co",
      };
      fs.writeFileSync(configPath, JSON.stringify(initialConfig));

      updateConfig(configPath, { baseUrl: "https://api.us-east.tinybird.co" });

      const updatedConfig = JSON.parse(fs.readFileSync(configPath, "utf-8"));
      expect(updatedConfig.baseUrl).toBe("https://api.us-east.tinybird.co");
      expect(updatedConfig.schema).toBe("lib/tinybird.ts");
      expect(updatedConfig.token).toBe("${TINYBIRD_TOKEN}");
    });

    it("throws error when config file does not exist", () => {
      const configPath = path.join(tempDir, "nonexistent.json");

      expect(() => updateConfig(configPath, { baseUrl: "test" })).toThrow(
        "Config not found"
      );
    });
  });

  describe("hasValidToken", () => {
    it("returns false when no config exists", () => {
      expect(hasValidToken(tempDir)).toBe(false);
    });

    it("returns true when token is literal value", () => {
      const config = {
        schema: "lib/tinybird.ts",
        token: "p.some-token",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(hasValidToken(tempDir)).toBe(true);
    });

    it("returns true when token env var is set", () => {
      process.env.HAS_VALID_TOKEN_TEST = "some-value";
      const config = {
        schema: "lib/tinybird.ts",
        token: "${HAS_VALID_TOKEN_TEST}",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(hasValidToken(tempDir)).toBe(true);
      delete process.env.HAS_VALID_TOKEN_TEST;
    });

    it("returns false when token env var is not set", () => {
      delete process.env.MISSING_TOKEN_VAR;
      const config = {
        schema: "lib/tinybird.ts",
        token: "${MISSING_TOKEN_VAR}",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(hasValidToken(tempDir)).toBe(false);
    });

    it("returns false when token field is empty", () => {
      const config = {
        schema: "lib/tinybird.ts",
        token: "",
      };
      fs.writeFileSync(
        path.join(tempDir, "tinybird.json"),
        JSON.stringify(config)
      );

      expect(hasValidToken(tempDir)).toBe(false);
    });
  });
});
