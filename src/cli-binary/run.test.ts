import { mkdtempSync, realpathSync } from "node:fs";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";

import { runTbBinary } from "./run.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FAKE_TB = join(__dirname, "..", "test", "fixtures", "fake-tb-cli.mjs");

// The fixture is a POSIX shebang script; skip on Windows where it can't be
// exec'd directly (see fake-tb-cli.mjs for why this test runs the real
// subprocess path instead of mocking child_process).
const itPosix = process.platform === "win32" ? it.skip : it;

describe("runTbBinary", () => {
  itPosix("captures stdout, stderr, and exit code", async () => {
    const result = await runTbBinary(FAKE_TB, ["deployment", "create"], {
      env: { FAKE_TB_STDOUT: '{"ok":true}', FAKE_TB_STDERR: "a warning", FAKE_TB_EXIT_CODE: "0" },
    });

    expect(result.stdout).toBe('{"ok":true}');
    expect(result.stderr).toBe("a warning");
    expect(result.exitCode).toBe(0);
  });

  itPosix("propagates a non-zero exit code", async () => {
    const result = await runTbBinary(FAKE_TB, ["deployment", "create"], {
      env: { FAKE_TB_STDERR: "boom", FAKE_TB_EXIT_CODE: "1" },
    });

    expect(result.exitCode).toBe(1);
    expect(result.stderr).toBe("boom");
  });

  itPosix("puts global flags before the subcommand, and credentials in env not argv", async () => {
    const result = await runTbBinary(FAKE_TB, ["deployment", "create", "--no-wait"], {
      env: { FAKE_TB_ECHO_ARGV: "1" },
      token: "p.secret-token",
      host: "https://api.tinybird.co",
      branch: "my-branch",
    });

    const parsed = JSON.parse(result.stdout);
    expect(parsed.argv).toEqual([
      "--output",
      "json",
      "--branch",
      "my-branch",
      "deployment",
      "create",
      "--no-wait",
    ]);
    expect(parsed.env.TB_TOKEN).toBe("p.secret-token");
    expect(parsed.env.TB_HOST).toBe("https://api.tinybird.co");
    // The token must never appear as a bare argv entry.
    expect(parsed.argv.join(" ")).not.toContain("p.secret-token");
  });

  itPosix("runs in the requested cwd", async () => {
    const cwd = realpathSync(mkdtempSync(join(tmpdir(), "run-tb-binary-test-")));
    const result = await runTbBinary(FAKE_TB, [], {
      env: { FAKE_TB_ECHO_ARGV: "1" },
      cwd,
    });

    const parsed = JSON.parse(result.stdout);
    expect(parsed.cwd).toBe(cwd);
  });
});
