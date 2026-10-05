/**
 * Thin spawn wrapper around the `tb` binary. Credentials are passed via env
 * vars (TB_TOKEN/TB_USER_TOKEN/TB_HOST), not CLI flags, so they don't show up
 * in `ps`/process listings, and never via the `.tinyb` config file, so
 * concurrent invocations don't race on shared state. See `CLIConfig` in
 * tinybird/tb/modules/config.py for the env vars the binary honors.
 *
 * Global flags (--output, --branch, ...) must precede the subcommand per
 * Click's group/subcommand parsing rules — see `cli()` in
 * tinybird/tb/modules/cli.py.
 */

import { spawn } from "node:child_process";

export interface RunTbOptions {
  cwd?: string;
  token?: string;
  userToken?: string;
  host?: string;
  branch?: string;
  outputFormat?: "human" | "json" | "csv";
  env?: NodeJS.ProcessEnv;
}

export interface RunTbResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
}

export function runTbBinary(
  binaryPath: string,
  commandArgs: string[],
  options: RunTbOptions = {}
): Promise<RunTbResult> {
  return new Promise((resolve, reject) => {
    const env: NodeJS.ProcessEnv = { ...process.env, ...options.env };
    if (options.token) env.TB_TOKEN = options.token;
    if (options.userToken) env.TB_USER_TOKEN = options.userToken;
    if (options.host) env.TB_HOST = options.host;

    const globalArgs = ["--output", options.outputFormat ?? "json"];
    if (options.branch) globalArgs.push("--branch", options.branch);

    const fullArgs = [...globalArgs, ...commandArgs];

    const child = spawn(binaryPath, fullArgs, { cwd: options.cwd, env });

    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf-8");
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf-8");
    });
    child.once("error", reject);
    child.once("close", (code) => {
      resolve({ stdout, stderr, exitCode: code });
    });
  });
}
