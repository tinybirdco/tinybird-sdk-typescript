/**
 * Resolves a usable `tb` binary path: an explicit TB_CLI_BINARY_PATH override,
 * or the manifest-published binary for the current platform, downloaded and
 * cached on demand.
 *
 * DRAFT: there is no published manifest yet (the MR that builds these
 * binaries, https://gitlab.com/tinybird/analytics/-/merge_requests/23765,
 * hasn't merged). This module is unit-tested against fakes only; nothing here
 * has been exercised against a real manifest or a real binary.
 */

export * from "./platform.js";
export * from "./manifest.js";
export * from "./download.js";
export * from "./run.js";

import { getHostPlatform } from "./platform.js";
import { fetchManifest, findArtifactForHost } from "./manifest.js";
import { ensureBinaryDownloaded } from "./download.js";

/**
 * `tb` version this SDK is tested against. "latest" is a placeholder until a
 * concrete version has actually been validated end-to-end (see plan: pin
 * deliberately once that validation happens, don't float on latest forever).
 */
export const DEFAULT_TB_CLI_VERSION = "latest";

export class CliBinaryUnavailableError extends Error {}

export interface ResolveTbBinaryOptions {
  version?: string;
  baseUrl?: string;
  fetchFn?: typeof fetch;
  cacheDir?: string;
}

export async function resolveTbBinaryPath(options: ResolveTbBinaryOptions = {}): Promise<string> {
  const override = process.env.TB_CLI_BINARY_PATH;
  if (override) {
    return override;
  }

  const host = getHostPlatform();
  const requestedVersion = options.version ?? process.env.TB_CLI_VERSION ?? DEFAULT_TB_CLI_VERSION;

  const manifest = await fetchManifest({
    baseUrl: options.baseUrl,
    version: requestedVersion,
    fetchFn: options.fetchFn,
  }).catch((error: unknown) => {
    throw new CliBinaryUnavailableError(
      `Could not fetch Tinybird CLI manifest: ${(error as Error).message}`
    );
  });

  const artifact = findArtifactForHost(manifest, host);
  if (!artifact) {
    throw new CliBinaryUnavailableError(
      `No Tinybird CLI binary is published for ${host.os}-${host.arch}. ` +
        `Set TB_CLI_BINARY_PATH to point at a local tb executable instead.`
    );
  }

  return ensureBinaryDownloaded({
    version: manifest.version,
    artifact,
    fetchFn: options.fetchFn,
    cacheDir: options.cacheDir,
  }).catch((error: unknown) => {
    throw new CliBinaryUnavailableError(
      `Could not download Tinybird CLI binary: ${(error as Error).message}`
    );
  });
}
