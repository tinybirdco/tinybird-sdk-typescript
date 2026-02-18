import { describe, it, expect } from "vitest";
import { clickhouseTypeToValidator, paramTypeToValidator } from "./type-mapper.js";

describe("clickhouseTypeToValidator", () => {
  describe("simple types", () => {
    it("maps String to t.string()", () => {
      expect(clickhouseTypeToValidator("String")).toBe("t.string()");
    });

    it("maps UUID to t.uuid()", () => {
      expect(clickhouseTypeToValidator("UUID")).toBe("t.uuid()");
    });

    it("maps integer types", () => {
      expect(clickhouseTypeToValidator("Int8")).toBe("t.int8()");
      expect(clickhouseTypeToValidator("Int16")).toBe("t.int16()");
      expect(clickhouseTypeToValidator("Int32")).toBe("t.int32()");
      expect(clickhouseTypeToValidator("Int64")).toBe("t.int64()");
      expect(clickhouseTypeToValidator("Int128")).toBe("t.int128()");
      expect(clickhouseTypeToValidator("Int256")).toBe("t.int256()");
    });

    it("maps unsigned integer types", () => {
      expect(clickhouseTypeToValidator("UInt8")).toBe("t.uint8()");
      expect(clickhouseTypeToValidator("UInt16")).toBe("t.uint16()");
      expect(clickhouseTypeToValidator("UInt32")).toBe("t.uint32()");
      expect(clickhouseTypeToValidator("UInt64")).toBe("t.uint64()");
      expect(clickhouseTypeToValidator("UInt128")).toBe("t.uint128()");
      expect(clickhouseTypeToValidator("UInt256")).toBe("t.uint256()");
    });

    it("maps float types", () => {
      expect(clickhouseTypeToValidator("Float32")).toBe("t.float32()");
      expect(clickhouseTypeToValidator("Float64")).toBe("t.float64()");
    });

    it("maps Bool to t.bool()", () => {
      expect(clickhouseTypeToValidator("Bool")).toBe("t.bool()");
      expect(clickhouseTypeToValidator("Boolean")).toBe("t.bool()");
    });

    it("maps date types", () => {
      expect(clickhouseTypeToValidator("Date")).toBe("t.date()");
      expect(clickhouseTypeToValidator("Date32")).toBe("t.date32()");
      expect(clickhouseTypeToValidator("DateTime")).toBe("t.dateTime()");
    });

    it("maps JSON to t.json()", () => {
      expect(clickhouseTypeToValidator("JSON")).toBe("t.json()");
    });

    it("maps IP types", () => {
      expect(clickhouseTypeToValidator("IPv4")).toBe("t.ipv4()");
      expect(clickhouseTypeToValidator("IPv6")).toBe("t.ipv6()");
    });
  });

  describe("Nullable wrapper", () => {
    it("adds .nullable() for Nullable(String)", () => {
      expect(clickhouseTypeToValidator("Nullable(String)")).toBe("t.string().nullable()");
    });

    it("adds .nullable() for Nullable(Int32)", () => {
      expect(clickhouseTypeToValidator("Nullable(Int32)")).toBe("t.int32().nullable()");
    });

    it("adds .nullable() for Nullable(DateTime)", () => {
      expect(clickhouseTypeToValidator("Nullable(DateTime)")).toBe("t.dateTime().nullable()");
    });
  });

  describe("LowCardinality wrapper", () => {
    it("adds .lowCardinality() for LowCardinality(String)", () => {
      expect(clickhouseTypeToValidator("LowCardinality(String)")).toBe("t.string().lowCardinality()");
    });

    it("handles LowCardinality(Nullable(String))", () => {
      expect(clickhouseTypeToValidator("LowCardinality(Nullable(String))")).toBe(
        "t.string().nullable().lowCardinality()"
      );
    });
  });

  describe("parameterized types", () => {
    it("handles DateTime with timezone", () => {
      expect(clickhouseTypeToValidator("DateTime('UTC')")).toBe('t.dateTime("UTC")');
      expect(clickhouseTypeToValidator("DateTime('America/New_York')")).toBe(
        't.dateTime("America/New_York")'
      );
    });

    it("handles DateTime64 with precision", () => {
      expect(clickhouseTypeToValidator("DateTime64(3)")).toBe("t.dateTime64(3)");
      expect(clickhouseTypeToValidator("DateTime64(6)")).toBe("t.dateTime64(6)");
    });

    it("handles DateTime64 with precision and timezone", () => {
      expect(clickhouseTypeToValidator("DateTime64(3, 'UTC')")).toBe('t.dateTime64(3, "UTC")');
    });

    it("handles FixedString(N)", () => {
      expect(clickhouseTypeToValidator("FixedString(10)")).toBe("t.fixedString(10)");
      expect(clickhouseTypeToValidator("FixedString(255)")).toBe("t.fixedString(255)");
    });

    it("handles Decimal(P, S)", () => {
      expect(clickhouseTypeToValidator("Decimal(10, 2)")).toBe("t.decimal(10, 2)");
      expect(clickhouseTypeToValidator("Decimal(18, 4)")).toBe("t.decimal(18, 4)");
    });
  });

  describe("complex types", () => {
    it("handles Array(T)", () => {
      expect(clickhouseTypeToValidator("Array(String)")).toBe("t.array(t.string())");
      expect(clickhouseTypeToValidator("Array(Int32)")).toBe("t.array(t.int32())");
    });

    it("handles nested Array types", () => {
      expect(clickhouseTypeToValidator("Array(Array(String))")).toBe(
        "t.array(t.array(t.string()))"
      );
    });

    it("handles Array with Nullable elements", () => {
      expect(clickhouseTypeToValidator("Array(Nullable(String))")).toBe(
        "t.array(t.string().nullable())"
      );
    });

    it("handles Map(K, V)", () => {
      expect(clickhouseTypeToValidator("Map(String, Int32)")).toBe(
        "t.map(t.string(), t.int32())"
      );
    });

    it("handles Tuple(T1, T2, ...)", () => {
      expect(clickhouseTypeToValidator("Tuple(String, Float64, String)")).toBe(
        "t.tuple(t.string(), t.float64(), t.string())"
      );
    });

    it("handles Array(Tuple(...))", () => {
      expect(clickhouseTypeToValidator("Array(Tuple(String, Float64, String))")).toBe(
        "t.array(t.tuple(t.string(), t.float64(), t.string()))"
      );
    });
  });

  describe("enum types", () => {
    it("handles Enum8", () => {
      expect(clickhouseTypeToValidator("Enum8('a' = 1, 'b' = 2)")).toBe(
        't.enum8("a", "b")'
      );
    });

    it("handles Enum16", () => {
      expect(clickhouseTypeToValidator("Enum16('pending' = 1, 'active' = 2, 'done' = 3)")).toBe(
        't.enum16("pending", "active", "done")'
      );
    });
  });

  describe("aggregate function types", () => {
    it("handles SimpleAggregateFunction", () => {
      expect(clickhouseTypeToValidator("SimpleAggregateFunction(sum, UInt64)")).toBe(
        't.simpleAggregateFunction("sum", t.uint64())'
      );
    });

    it("handles AggregateFunction", () => {
      expect(clickhouseTypeToValidator("AggregateFunction(uniq, String)")).toBe(
        't.aggregateFunction("uniq", t.string())'
      );
    });

    it("handles AggregateFunction(count) without explicit state type", () => {
      expect(clickhouseTypeToValidator("AggregateFunction(count)")).toBe(
        't.aggregateFunction("count", t.uint64())'
      );
    });
  });

  describe("unknown types", () => {
    it("returns string with TODO comment for unknown types", () => {
      expect(clickhouseTypeToValidator("UnknownType")).toBe(
        "t.string() /* TODO: Unknown type: UnknownType */"
      );
    });
  });
});

describe("paramTypeToValidator", () => {
  describe("simple types", () => {
    it("maps String to p.string()", () => {
      expect(paramTypeToValidator("String")).toBe("p.string()");
    });

    it("maps Int32 to p.int32()", () => {
      expect(paramTypeToValidator("Int32")).toBe("p.int32()");
    });

    it("maps DateTime to p.dateTime()", () => {
      expect(paramTypeToValidator("DateTime")).toBe("p.dateTime()");
    });

    it("maps Boolean to p.boolean()", () => {
      expect(paramTypeToValidator("Boolean")).toBe("p.boolean()");
      expect(paramTypeToValidator("Bool")).toBe("p.boolean()");
    });
  });

  describe("optional parameters", () => {
    it("adds .optional() for non-required params", () => {
      expect(paramTypeToValidator("String", undefined, false)).toBe("p.string().optional()");
    });

    it("adds .optional(default) for params with defaults", () => {
      expect(paramTypeToValidator("Int32", 10)).toBe("p.int32().optional(10)");
      expect(paramTypeToValidator("String", "test")).toBe('p.string().optional("test")');
    });

    it("adds .optional(default) even for required params with defaults", () => {
      expect(paramTypeToValidator("Int32", 5, true)).toBe("p.int32().optional(5)");
    });
  });

  describe("DateTime variants", () => {
    it("handles DateTime64", () => {
      expect(paramTypeToValidator("DateTime64")).toBe("p.dateTime64()");
    });

    it("handles DateTime with timezone", () => {
      expect(paramTypeToValidator("DateTime('UTC')")).toBe("p.dateTime()");
    });
  });

  describe("unknown types", () => {
    it("defaults to p.string() for unknown types", () => {
      expect(paramTypeToValidator("UnknownType")).toBe("p.string()");
    });
  });
});
