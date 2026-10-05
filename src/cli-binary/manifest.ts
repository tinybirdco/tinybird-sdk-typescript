/**
 * Fetches and parses the manifest published by the `tb` CLI binary pipeline
 * (MR https://gitlab.com/tinybird/analytics/-/merge_requests/23765).
 *
 * Manifest shape is produced by `tinybird.tb.modules.binary.build_manifest`:
 * `{ version, generated_at, artifacts: [{ id, os, arch, bundle_mode,
 * executable_relative_path, filename, sha256, size_bytes, relative_path, url }] }`.
 */

import type { HostPlatform } from "./platform.js";

export interface ManifestArtifact {
  id: string;
  os: string;
  arch: string;
  bundle_mode: string;
  executable_relative_path: string;
  filename: string;
  sha256: string;
  size_bytes: number;
  relative_path: string;
  /** Absent when the manifest was generated without --base-url. */
  url?: string;
}

export interface CliManifest {
  version: string;
  generated_at: string;
  artifacts: ManifestArtifact[];
}

/**
 * Default base URL for the published manifest (`${base}/${version}/manifest.json`).
 * Matches the CLI_BINARIES_BASE_URL CI variable used when publishing (see
 * gitlab/ci-templates/cli.yml in tinybird/analytics).
 */
export const DEFAULT_CLI_BINARIES_BASE_URL = "https://tinybird.co/cli";

export interface FetchManifestOptions {
  /** Defaults to TB_CLI_BASE_URL env var, then DEFAULT_CLI_BINARIES_BASE_URL. */
  baseUrl?: string;
  /** Defaults to "latest". */
  version?: string;
  fetchFn?: typeof fetch;
}

export async function fetchManifest(options: FetchManifestOptions = {}): Promise<CliManifest> {
  const baseUrl = (options.baseUrl ?? process.env.TB_CLI_BASE_URL ?? DEFAULT_CLI_BINARIES_BASE_URL).replace(
    /\/$/,
    ""
  );
  const version = options.version ?? "latest";
  const fetchFn = options.fetchFn ?? fetch;
  const url = `${baseUrl}/${version}/manifest.json`;

  const response = await fetchFn(url);
  if (!response.ok) {
    throw new Error(
      `Failed to fetch Tinybird CLI manifest from ${url}: ${response.status} ${response.statusText}`
    );
  }

  const manifest = (await response.json()) as CliManifest;
  if (!manifest || !Array.isArray(manifest.artifacts) || typeof manifest.version !== "string") {
    throw new Error(`Invalid Tinybird CLI manifest received from ${url}`);
  }

  return manifest;
}

export function findArtifactForHost(
  manifest: CliManifest,
  host: HostPlatform
): ManifestArtifact | undefined {
  return manifest.artifacts.find((artifact) => artifact.os === host.os && artifact.arch === host.arch);
}
