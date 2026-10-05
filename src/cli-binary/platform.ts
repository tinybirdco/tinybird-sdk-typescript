/**
 * Platform/arch normalization matching the Python side of the binary pipeline
 * (`tinybird.tb.modules.binary.normalize_os` / `normalize_arch` in
 * tinybird/analytics, see MR !23765) so manifest target ids line up exactly.
 */

export interface HostPlatform {
  os: string;
  arch: string;
}

export function normalizeOs(rawOs: string): string {
  const value = rawOs.toLowerCase();
  if (value.startsWith("linux")) return "linux";
  if (value.startsWith("darwin")) return "darwin";
  if (value.startsWith("win")) return "windows";
  return value;
}

export function normalizeArch(rawArch: string): string {
  const value = rawArch.toLowerCase();
  if (value === "x86_64" || value === "amd64" || value === "x64") return "x64";
  if (value === "arm64" || value === "aarch64") return "arm64";
  return value;
}

export function getHostPlatform(): HostPlatform {
  return {
    os: normalizeOs(process.platform),
    arch: normalizeArch(process.arch),
  };
}

export function executableName(targetOs: string): string {
  return targetOs === "windows" ? "tb.exe" : "tb";
}
