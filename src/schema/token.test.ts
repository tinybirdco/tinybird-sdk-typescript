import { describe, it, expect } from "vitest";
import { defineToken, isTokenDefinition } from "./token.js";

describe("Token Schema", () => {
  describe("defineToken", () => {
    it("creates a token with a valid name", () => {
      const token = defineToken("app_read");

      expect(token._name).toBe("app_read");
      expect(token._type).toBe("token");
    });

    it("allows underscores in token names", () => {
      const token = defineToken("my_app_token");
      expect(token._name).toBe("my_app_token");
    });

    it("allows leading underscore in token names", () => {
      const token = defineToken("_private_token");
      expect(token._name).toBe("_private_token");
    });

    it("allows alphanumeric characters in token names", () => {
      const token = defineToken("token_v2");
      expect(token._name).toBe("token_v2");
    });

    it("throws error for invalid token name starting with number", () => {
      expect(() => defineToken("123token")).toThrow("Invalid token name");
    });

    it("throws error for invalid token name with hyphens", () => {
      expect(() => defineToken("my-token")).toThrow("Invalid token name");
    });

    it("throws error for empty token name", () => {
      expect(() => defineToken("")).toThrow("Invalid token name");
    });

    it("throws error for token name with spaces", () => {
      expect(() => defineToken("my token")).toThrow("Invalid token name");
    });
  });

  describe("isTokenDefinition", () => {
    it("returns true for valid token", () => {
      const token = defineToken("app_read");
      expect(isTokenDefinition(token)).toBe(true);
    });

    it("returns false for non-token objects", () => {
      expect(isTokenDefinition({})).toBe(false);
      expect(isTokenDefinition(null)).toBe(false);
      expect(isTokenDefinition(undefined)).toBe(false);
      expect(isTokenDefinition("string")).toBe(false);
      expect(isTokenDefinition(123)).toBe(false);
      expect(isTokenDefinition({ _name: "test" })).toBe(false);
    });
  });
});
