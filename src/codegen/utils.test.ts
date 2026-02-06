import { describe, it, expect } from "vitest";
import {
  toCamelCase,
  toPascalCase,
  escapeString,
  parseSortingKey,
  generateEngineCode,
  formatSqlForTemplate,
} from "./utils.js";

describe("toCamelCase", () => {
  it("converts snake_case to camelCase", () => {
    expect(toCamelCase("page_views")).toBe("pageViews");
    expect(toCamelCase("user_session_data")).toBe("userSessionData");
  });

  it("converts kebab-case to camelCase", () => {
    expect(toCamelCase("page-views")).toBe("pageViews");
    expect(toCamelCase("user-session-data")).toBe("userSessionData");
  });

  it("lowercases first character of PascalCase", () => {
    expect(toCamelCase("PageViews")).toBe("pageViews");
  });

  it("handles single word", () => {
    expect(toCamelCase("events")).toBe("events");
  });

  it("prefixes reserved keywords with underscore", () => {
    expect(toCamelCase("class")).toBe("_class");
    expect(toCamelCase("function")).toBe("_function");
    expect(toCamelCase("return")).toBe("_return");
    expect(toCamelCase("import")).toBe("_import");
    expect(toCamelCase("export")).toBe("_export");
  });

  it("prefixes names starting with numbers", () => {
    expect(toCamelCase("123_test")).toBe("_123Test");
    expect(toCamelCase("1events")).toBe("_1events");
  });
});

describe("toPascalCase", () => {
  it("converts snake_case to PascalCase", () => {
    expect(toPascalCase("page_views")).toBe("PageViews");
    expect(toPascalCase("user_session_data")).toBe("UserSessionData");
  });

  it("converts kebab-case to PascalCase", () => {
    expect(toPascalCase("page-views")).toBe("PageViews");
  });

  it("uppercases first character", () => {
    expect(toPascalCase("events")).toBe("Events");
  });
});

describe("escapeString", () => {
  it("escapes double quotes", () => {
    expect(escapeString('hello "world"')).toBe('hello \\"world\\"');
  });

  it("escapes backslashes", () => {
    expect(escapeString("path\\to\\file")).toBe("path\\\\to\\\\file");
  });

  it("escapes newlines", () => {
    expect(escapeString("line1\nline2")).toBe("line1\\nline2");
  });

  it("escapes tabs", () => {
    expect(escapeString("col1\tcol2")).toBe("col1\\tcol2");
  });

  it("handles combined escapes", () => {
    expect(escapeString('say "hello\\world"\n')).toBe('say \\"hello\\\\world\\"\\n');
  });
});

describe("parseSortingKey", () => {
  it("parses single column", () => {
    expect(parseSortingKey("timestamp")).toEqual(["timestamp"]);
  });

  it("parses multiple columns", () => {
    expect(parseSortingKey("user_id, timestamp")).toEqual(["user_id", "timestamp"]);
  });

  it("trims whitespace", () => {
    expect(parseSortingKey("  user_id  ,  timestamp  ")).toEqual(["user_id", "timestamp"]);
  });

  it("returns empty array for undefined", () => {
    expect(parseSortingKey(undefined)).toEqual([]);
  });

  it("returns empty array for empty string", () => {
    expect(parseSortingKey("")).toEqual([]);
  });
});

describe("generateEngineCode", () => {
  it("generates MergeTree engine code", () => {
    const code = generateEngineCode({
      type: "MergeTree",
      sorting_key: "timestamp",
    });
    expect(code).toContain("engine.mergeTree");
    expect(code).toContain('sortingKey: "timestamp"');
  });

  it("generates MergeTree with multiple sorting keys", () => {
    const code = generateEngineCode({
      type: "MergeTree",
      sorting_key: "user_id, timestamp",
    });
    expect(code).toContain("engine.mergeTree");
    expect(code).toContain('sortingKey: ["user_id", "timestamp"]');
  });

  it("includes partition key", () => {
    const code = generateEngineCode({
      type: "MergeTree",
      sorting_key: "timestamp",
      partition_key: "toYYYYMM(timestamp)",
    });
    expect(code).toContain('partitionKey: "toYYYYMM(timestamp)"');
  });

  it("includes TTL", () => {
    const code = generateEngineCode({
      type: "MergeTree",
      sorting_key: "timestamp",
      ttl: "timestamp + INTERVAL 90 DAY",
    });
    expect(code).toContain('ttl: "timestamp + INTERVAL 90 DAY"');
  });

  it("generates ReplacingMergeTree with ver column", () => {
    const code = generateEngineCode({
      type: "ReplacingMergeTree",
      sorting_key: "id",
      ver: "updated_at",
    });
    expect(code).toContain("engine.replacingMergeTree");
    expect(code).toContain('ver: "updated_at"');
  });

  it("generates SummingMergeTree with columns", () => {
    const code = generateEngineCode({
      type: "SummingMergeTree",
      sorting_key: "date, category",
      summing_columns: "count, total",
    });
    expect(code).toContain("engine.summingMergeTree");
    expect(code).toContain('columns: ["count", "total"]');
  });

  it("generates AggregatingMergeTree", () => {
    const code = generateEngineCode({
      type: "AggregatingMergeTree",
      sorting_key: "date",
    });
    expect(code).toContain("engine.aggregatingMergeTree");
  });

  it("generates CollapsingMergeTree with sign column", () => {
    const code = generateEngineCode({
      type: "CollapsingMergeTree",
      sorting_key: "id, timestamp",
      sign: "sign",
    });
    expect(code).toContain("engine.collapsingMergeTree");
    expect(code).toContain('sign: "sign"');
  });

  it("generates VersionedCollapsingMergeTree", () => {
    const code = generateEngineCode({
      type: "VersionedCollapsingMergeTree",
      sorting_key: "id",
      sign: "sign",
      version: "version",
    });
    expect(code).toContain("engine.versionedCollapsingMergeTree");
    expect(code).toContain('sign: "sign"');
    expect(code).toContain('version: "version"');
  });

  it("defaults to mergeTree for unknown engine types", () => {
    const code = generateEngineCode({
      type: "UnknownEngine",
      sorting_key: "id",
    });
    expect(code).toContain("engine.mergeTree");
  });
});

describe("formatSqlForTemplate", () => {
  it("escapes backticks", () => {
    expect(formatSqlForTemplate("SELECT `column` FROM table")).toBe(
      "SELECT \\`column\\` FROM table"
    );
  });

  it("escapes template literal interpolations", () => {
    expect(formatSqlForTemplate("SELECT ${column} FROM table")).toBe(
      "SELECT \\${column} FROM table"
    );
  });

  it("preserves newlines", () => {
    const sql = "SELECT *\nFROM table\nWHERE id = 1";
    expect(formatSqlForTemplate(sql)).toBe(sql);
  });

  it("preserves Tinybird template syntax", () => {
    const sql = "SELECT * FROM table WHERE id = {{Int32(id)}}";
    expect(formatSqlForTemplate(sql)).toBe(sql);
  });
});
