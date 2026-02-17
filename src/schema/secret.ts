/**
 * Secret template helper.
 * Produces Tinybird-compatible `tb_secret(...)` template strings.
 */
export function secret(name: string, defaultValue?: string): string {
  if (!name || name.trim().length === 0) {
    throw new Error("Secret name must be a non-empty string.");
  }

  if (defaultValue === undefined) {
    return `{{ tb_secret("${name}") }}`;
  }

  return `{{ tb_secret("${name}", "${defaultValue}") }}`;
}

