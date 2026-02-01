import { describe, it, expect } from "vitest";
import {
  defineDatasource,
  isDatasourceDefinition,
  getColumnType,
  getColumnJsonPath,
  getColumnNames,
  column,
} from "./datasource.js";
import { t } from "./types.js";
import { engine } from "./engines.js";

describe("Datasource Schema", () => {
  describe("defineDatasource", () => {
    it("creates a datasource with required fields", () => {
      const ds = defineDatasource("events", {
        schema: {
          id: t.string(),
          timestamp: t.dateTime(),
        },
      });

      expect(ds._name).toBe("events");
      expect(ds._type).toBe("datasource");
      expect(ds.options.schema).toBeDefined();
    });

    it("creates a datasource with description", () => {
      const ds = defineDatasource("events", {
        description: "Event tracking data",
        schema: {
          id: t.string(),
        },
      });

      expect(ds.options.description).toBe("Event tracking data");
    });

    it("creates a datasource with engine configuration", () => {
      const ds = defineDatasource("events", {
        schema: {
          id: t.string(),
          timestamp: t.dateTime(),
        },
        engine: engine.mergeTree({
          sortingKey: ["id", "timestamp"],
          partitionKey: "toYYYYMM(timestamp)",
        }),
      });

      expect(ds.options.engine).toBeDefined();
      expect(ds.options.engine?.type).toBe("MergeTree");
    });

    it("throws error for invalid datasource name", () => {
      expect(() =>
        defineDatasource("123invalid", {
          schema: { id: t.string() },
        })
      ).toThrow("Invalid datasource name");

      expect(() =>
        defineDatasource("my-datasource", {
          schema: { id: t.string() },
        })
      ).toThrow("Invalid datasource name");

      expect(() =>
        defineDatasource("", {
          schema: { id: t.string() },
        })
      ).toThrow("Invalid datasource name");
    });

    it("allows valid naming patterns", () => {
      // Underscore prefix
      const ds1 = defineDatasource("_private", {
        schema: { id: t.string() },
      });
      expect(ds1._name).toBe("_private");

      // With numbers
      const ds2 = defineDatasource("events_v2", {
        schema: { id: t.string() },
      });
      expect(ds2._name).toBe("events_v2");
    });
  });

  describe("isDatasourceDefinition", () => {
    it("returns true for valid datasource", () => {
      const ds = defineDatasource("events", {
        schema: { id: t.string() },
      });

      expect(isDatasourceDefinition(ds)).toBe(true);
    });

    it("returns false for non-datasource objects", () => {
      expect(isDatasourceDefinition({})).toBe(false);
      expect(isDatasourceDefinition(null)).toBe(false);
      expect(isDatasourceDefinition(undefined)).toBe(false);
      expect(isDatasourceDefinition("string")).toBe(false);
      expect(isDatasourceDefinition(123)).toBe(false);
      expect(isDatasourceDefinition({ _name: "test" })).toBe(false);
    });
  });

  describe("getColumnType", () => {
    it("returns type from raw validator", () => {
      const validator = t.string();
      const result = getColumnType(validator);

      expect(result).toBe(validator);
    });

    it("returns type from column definition", () => {
      const validator = t.string();
      const col = column(validator, { jsonPath: "$.id" });
      const result = getColumnType(col);

      expect(result).toBe(validator);
    });
  });

  describe("getColumnJsonPath", () => {
    it("returns undefined for raw validator", () => {
      const validator = t.string();
      const result = getColumnJsonPath(validator);

      expect(result).toBeUndefined();
    });

    it("returns jsonPath from column definition", () => {
      const col = column(t.string(), { jsonPath: "$.user.id" });
      const result = getColumnJsonPath(col);

      expect(result).toBe("$.user.id");
    });

    it("returns undefined when jsonPath is not set", () => {
      const col = column(t.string());
      const result = getColumnJsonPath(col);

      expect(result).toBeUndefined();
    });
  });

  describe("getColumnNames", () => {
    it("returns all column names from schema", () => {
      const schema = {
        id: t.string(),
        timestamp: t.dateTime(),
        user_id: t.string(),
      };

      const names = getColumnNames(schema);

      expect(names).toHaveLength(3);
      expect(names).toContain("id");
      expect(names).toContain("timestamp");
      expect(names).toContain("user_id");
    });

    it("returns empty array for empty schema", () => {
      const names = getColumnNames({});

      expect(names).toHaveLength(0);
    });
  });

  describe("column", () => {
    it("creates a column definition with type only", () => {
      const col = column(t.string());

      expect(col.type).toBeDefined();
      expect(col.jsonPath).toBeUndefined();
    });

    it("creates a column definition with jsonPath", () => {
      const col = column(t.string(), { jsonPath: "$.data.value" });

      expect(col.type).toBeDefined();
      expect(col.jsonPath).toBe("$.data.value");
    });
  });
});
