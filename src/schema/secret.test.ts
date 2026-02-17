import { describe, expect, it } from "vitest";
import { secret } from "./secret.js";

describe("secret helper", () => {
  it("creates a secret template without default", () => {
    expect(secret("KAFKA_KEY")).toBe('{{ tb_secret("KAFKA_KEY") }}');
  });

  it("creates a secret template with default", () => {
    expect(secret("KAFKA_GROUP_ID", "events_group")).toBe(
      '{{ tb_secret("KAFKA_GROUP_ID", "events_group") }}'
    );
  });

  it("throws on empty secret name", () => {
    expect(() => secret("")).toThrow("Secret name must be a non-empty string.");
  });
});

