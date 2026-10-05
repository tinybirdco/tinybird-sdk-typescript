#!/usr/bin/env node
/**
 * Stand-in for the real `tb` binary, used only in tests. Mirrors the Python
 * side's own `tests_tb/fixtures/fake_pyinstaller` pattern (MR !23765): the
 * subprocess call in run.ts is real and unmocked, but it spawns this
 * lightweight script instead of a multi-hundred-MB PyInstaller binary.
 *
 * Behavior is controlled entirely by env vars so callers don't need a second
 * fixture file per scenario:
 * - FAKE_TB_STDOUT: raw string written to stdout (default: "").
 * - FAKE_TB_STDERR: raw string written to stderr (default: "").
 * - FAKE_TB_EXIT_CODE: process exit code (default: 0).
 * - FAKE_TB_ECHO_ARGV: if set, ignores the above and instead writes
 *   `{ argv, cwd, env, projectFiles }` (argv/cwd/the env vars tb itself
 *   reads/the datasources|pipes|connections files found under cwd, as
 *   "folder/name" paths) as JSON to stdout and exits 0 — used to assert what
 *   run.ts passed through and what the SDK materialized on disk for tb to
 *   read.
 */
import { readdirSync } from "node:fs";
import process from "node:process";

function listProjectFiles(cwd) {
  const files = [];
  for (const folder of ["datasources", "pipes", "connections"]) {
    try {
      for (const name of readdirSync(`${cwd}/${folder}`)) {
        files.push(`${folder}/${name}`);
      }
    } catch {
      // folder doesn't exist for this resource kind — fine.
    }
  }
  return files.sort();
}

if (process.env.FAKE_TB_ECHO_ARGV) {
  const cwd = process.cwd();
  process.stdout.write(
    JSON.stringify({
      argv: process.argv.slice(2),
      cwd,
      env: {
        TB_TOKEN: process.env.TB_TOKEN ?? null,
        TB_USER_TOKEN: process.env.TB_USER_TOKEN ?? null,
        TB_HOST: process.env.TB_HOST ?? null,
      },
      projectFiles: listProjectFiles(cwd),
    })
  );
  process.exit(0);
}

if (process.env.FAKE_TB_STDOUT) {
  process.stdout.write(process.env.FAKE_TB_STDOUT);
}
if (process.env.FAKE_TB_STDERR) {
  process.stderr.write(process.env.FAKE_TB_STDERR);
}
process.exit(process.env.FAKE_TB_EXIT_CODE ? Number.parseInt(process.env.FAKE_TB_EXIT_CODE, 10) : 0);
