/**
 * Tests for schema validation utility
 */

import { describe, it, expect } from "vitest";
import {
  _typesAreCompatible,
  _validateOutputSchema,
  _hasRequiredParams,
  _buildDefaultParams,
} from "./schema-validation.js";
import type { PipeDefinition } from "../../schema/pipe.js";
import type { ColumnMeta } from "../../client/types.js";

describe("typesAreCompatible", () => {
  it("matches identical types", () => {
    expect(_typesAreCompatible("String", "String")).toBe(true);
    expect(_typesAreCompatible("Int64", "Int64")).toBe(true);
    expect(_typesAreCompatible("UInt64", "UInt64")).toBe(true);
  });

  it("matches Nullable wrapped types", () => {
    expect(_typesAreCompatible("Nullable(String)", "String")).toBe(true);
    expect(_typesAreCompatible("Nullable(Int64)", "Int64")).toBe(true);
  });

  it("matches LowCardinality wrapped types", () => {
    expect(_typesAreCompatible("LowCardinality(String)", "String")).toBe(true);
  });

  it("matches LowCardinality(Nullable(...)) wrapped types", () => {
    expect(
      _typesAreCompatible("LowCardinality(Nullable(String))", "String")
    ).toBe(true);
  });

  it("matches DateTime with timezone to base DateTime", () => {
    expect(_typesAreCompatible("DateTime('UTC')", "DateTime")).toBe(true);
    expect(_typesAreCompatible("DateTime('Europe/Madrid')", "DateTime")).toBe(
      true
    );
  });

  it("matches DateTime64 with precision", () => {
    expect(_typesAreCompatible("DateTime64(3)", "DateTime64")).toBe(true);
    expect(_typesAreCompatible("DateTime64(6, 'UTC')", "DateTime64")).toBe(
      true
    );
  });

  it("rejects mismatched types", () => {
    expect(_typesAreCompatible("String", "Int64")).toBe(false);
    expect(_typesAreCompatible("UInt64", "Int64")).toBe(false);
    expect(_typesAreCompatible("DateTime", "Date")).toBe(false);
  });
});

describe("validateOutputSchema", () => {
  it("validates matching schema", () => {
    const responseMeta: ColumnMeta[] = [
      { name: "id", type: "UInt64" },
      { name: "name", type: "String" },
    ];

    const outputSchema = {
      id: { _tinybirdType: "UInt64" },
      name: { _tinybirdType: "String" },
    };

    const result = _validateOutputSchema(responseMeta, outputSchema as any);

    expect(result.valid).toBe(true);
    expect(result.missingColumns).toHaveLength(0);
    expect(result.extraColumns).toHaveLength(0);
    expect(result.typeMismatches).toHaveLength(0);
  });

  it("detects missing columns", () => {
    const responseMeta: ColumnMeta[] = [{ name: "id", type: "UInt64" }];

    const outputSchema = {
      id: { _tinybirdType: "UInt64" },
      name: { _tinybirdType: "String" },
    };

    const result = _validateOutputSchema(responseMeta, outputSchema as any);

    expect(result.valid).toBe(false);
    expect(result.missingColumns).toHaveLength(1);
    expect(result.missingColumns[0]).toEqual({
      name: "name",
      expectedType: "String",
    });
  });

  it("detects extra columns", () => {
    const responseMeta: ColumnMeta[] = [
      { name: "id", type: "UInt64" },
      { name: "extra", type: "String" },
    ];

    const outputSchema = {
      id: { _tinybirdType: "UInt64" },
    };

    const result = _validateOutputSchema(responseMeta, outputSchema as any);

    // Extra columns are warnings, not errors
    expect(result.valid).toBe(true);
    expect(result.extraColumns).toHaveLength(1);
    expect(result.extraColumns[0]).toEqual({
      name: "extra",
      actualType: "String",
    });
  });

  it("detects type mismatches", () => {
    const responseMeta: ColumnMeta[] = [{ name: "count", type: "Int64" }];

    const outputSchema = {
      count: { _tinybirdType: "UInt64" },
    };

    const result = _validateOutputSchema(responseMeta, outputSchema as any);

    expect(result.valid).toBe(false);
    expect(result.typeMismatches).toHaveLength(1);
    expect(result.typeMismatches[0]).toEqual({
      name: "count",
      expectedType: "UInt64",
      actualType: "Int64",
    });
  });

  it("handles Nullable types as compatible", () => {
    const responseMeta: ColumnMeta[] = [
      { name: "value", type: "Nullable(String)" },
    ];

    const outputSchema = {
      value: { _tinybirdType: "String" },
    };

    const result = _validateOutputSchema(responseMeta, outputSchema as any);

    expect(result.valid).toBe(true);
    expect(result.typeMismatches).toHaveLength(0);
  });
});

describe("hasRequiredParams", () => {
  it("returns false for pipe with no params", () => {
    const pipe = { _name: "test" } as PipeDefinition;
    expect(_hasRequiredParams(pipe)).toBe(false);
  });

  it("returns false for pipe with only optional params", () => {
    const pipe = {
      _name: "test",
      _params: {
        limit: { _required: false, _default: 10 },
      },
    } as unknown as PipeDefinition;
    expect(_hasRequiredParams(pipe)).toBe(false);
  });

  it("returns true for pipe with required param without default", () => {
    const pipe = {
      _name: "test",
      _params: {
        start_date: { _required: true, _default: undefined },
      },
    } as unknown as PipeDefinition;
    expect(_hasRequiredParams(pipe)).toBe(true);
  });

  it("returns false for required param with default value", () => {
    const pipe = {
      _name: "test",
      _params: {
        start_date: { _required: true, _default: "2024-01-01" },
      },
    } as unknown as PipeDefinition;
    expect(_hasRequiredParams(pipe)).toBe(false);
  });
});

describe("buildDefaultParams", () => {
  it("returns empty object for pipe with no params", () => {
    const pipe = { _name: "test" } as PipeDefinition;
    expect(_buildDefaultParams(pipe)).toEqual({});
  });

  it("includes params with default values", () => {
    const pipe = {
      _name: "test",
      _params: {
        limit: { _required: false, _default: 10 },
        offset: { _required: false, _default: 0 },
      },
    } as unknown as PipeDefinition;

    expect(_buildDefaultParams(pipe)).toEqual({
      limit: 10,
      offset: 0,
    });
  });

  it("excludes params without default values", () => {
    const pipe = {
      _name: "test",
      _params: {
        limit: { _required: false, _default: 10 },
        start_date: { _required: true, _default: undefined },
      },
    } as unknown as PipeDefinition;

    expect(_buildDefaultParams(pipe)).toEqual({
      limit: 10,
    });
  });
});
