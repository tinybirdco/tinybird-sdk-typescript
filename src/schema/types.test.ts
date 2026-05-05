import { describe, it, expect, expectTypeOf } from "vitest";
import { t, isTypeValidator, getTinybirdType, getModifiers } from "./types.js";
import { defineDatasource } from "./datasource.js";
import { engine } from "./engines.js";
import type { InferRow } from "../infer/index.js";

describe("Type Validators (t.*)", () => {
  describe("Basic types", () => {
    it("generates String type", () => {
      const type = t.string();
      expect(type._tinybirdType).toBe("String");
    });

    it("generates Int32 type", () => {
      const type = t.int32();
      expect(type._tinybirdType).toBe("Int32");
    });

    it("generates DateTime type", () => {
      const type = t.dateTime();
      expect(type._tinybirdType).toBe("DateTime");
    });

    it("generates DateTime with timezone", () => {
      const type = t.dateTime("UTC");
      expect(type._tinybirdType).toBe("DateTime('UTC')");
    });

    it("generates Bool type", () => {
      const type = t.bool();
      expect(type._tinybirdType).toBe("Bool");
    });

    it("generates UUID type", () => {
      const type = t.uuid();
      expect(type._tinybirdType).toBe("UUID");
    });

    it("generates Float64 type", () => {
      const type = t.float64();
      expect(type._tinybirdType).toBe("Float64");
    });

    it("generates UInt64 type", () => {
      const type = t.uint64();
      expect(type._tinybirdType).toBe("UInt64");
    });
  });

  describe("Nullable modifier", () => {
    it("wraps type in Nullable", () => {
      const type = t.string().nullable();
      expect(type._tinybirdType).toBe("Nullable(String)");
    });

    it("wraps Int32 in Nullable", () => {
      const type = t.int32().nullable();
      expect(type._tinybirdType).toBe("Nullable(Int32)");
    });

    it("sets nullable modifier", () => {
      const type = t.string().nullable();
      expect(type._modifiers.nullable).toBe(true);
    });
  });

  describe("LowCardinality modifier", () => {
    it("wraps type in LowCardinality", () => {
      const type = t.string().lowCardinality();
      expect(type._tinybirdType).toBe("LowCardinality(String)");
    });

    it("sets lowCardinality modifier", () => {
      const type = t.string().lowCardinality();
      expect(type._modifiers.lowCardinality).toBe(true);
    });
  });

  describe("LowCardinality + Nullable ordering", () => {
    it("generates LowCardinality(Nullable(X)) when chaining .lowCardinality().nullable()", () => {
      const type = t.string().lowCardinality().nullable();
      expect(type._tinybirdType).toBe("LowCardinality(Nullable(String))");
    });

    it("generates LowCardinality(Nullable(X)) when chaining .nullable().lowCardinality()", () => {
      const type = t.string().nullable().lowCardinality();
      expect(type._tinybirdType).toBe("LowCardinality(Nullable(String))");
    });

    it("preserves lowCardinality modifier and omits nullable when combined (nullable is in the type string)", () => {
      const type = t.string().lowCardinality().nullable();
      expect(type._modifiers.lowCardinality).toBe(true);
      expect(type._modifiers.nullable).toBeUndefined();
      expect(type._tinybirdType).toBe("LowCardinality(Nullable(String))");
    });

    it("omits nullable modifier when nullable().lowCardinality() is chained", () => {
      const type = t.string().nullable().lowCardinality();
      expect(type._modifiers.lowCardinality).toBe(true);
      expect(type._modifiers.nullable).toBeUndefined();
      expect(type._tinybirdType).toBe("LowCardinality(Nullable(String))");
    });
  });

  describe("Default values", () => {
    it("sets hasDefault modifier", () => {
      const type = t.string().default("test");
      expect(type._modifiers.hasDefault).toBe(true);
    });

    it("stores defaultValue in modifiers", () => {
      const type = t.string().default("test");
      expect(type._modifiers.defaultValue).toBe("test");
    });

    it("works with numeric defaults", () => {
      const type = t.int32().default(42);
      expect(type._modifiers.defaultValue).toBe(42);
    });

    it("stores default SQL expression in modifiers", () => {
      const type = t.uuid().defaultExpr("generateUUIDv4()");
      expect(type._modifiers.hasDefault).toBe(true);
      expect(type._modifiers.defaultExpression).toBe("generateUUIDv4()");
      expect(type._modifiers.defaultValue).toBeUndefined();
    });

    it("trims default SQL expression", () => {
      const type = t.uuid().defaultExpr("  generateUUIDv4()  ");
      expect(type._modifiers.defaultExpression).toBe("generateUUIDv4()");
    });

    it("throws on empty default SQL expression", () => {
      expect(() => t.uuid().defaultExpr("   ")).toThrow(
        "Default expression cannot be empty."
      );
    });
  });

  describe("Codec modifier", () => {
    it("sets codec in modifiers", () => {
      const type = t.string().codec("LZ4");
      expect(type._modifiers.codec).toBe("LZ4");
    });
  });

  describe("jsonPath modifier", () => {
    it("sets jsonPath in modifiers", () => {
      const type = t.string().jsonPath("$.payload.id");
      expect(type._modifiers.jsonPath).toBe("$.payload.id");
    });

    it("supports chaining with other modifiers", () => {
      const type = t.string().nullable().jsonPath("$.user.name");
      expect(type._tinybirdType).toBe("Nullable(String)");
      expect(type._modifiers.nullable).toBe(true);
      expect(type._modifiers.jsonPath).toBe("$.user.name");
    });
  });

  describe("Complex types", () => {
    it("generates Array type", () => {
      const type = t.array(t.string());
      expect(type._tinybirdType).toBe("Array(String)");
    });

    it("generates nested Array type", () => {
      const type = t.array(t.int32());
      expect(type._tinybirdType).toBe("Array(Int32)");
    });

    it("generates Map type", () => {
      const type = t.map(t.string(), t.int32());
      expect(type._tinybirdType).toBe("Map(String, Int32)");
    });

    it("generates Decimal type", () => {
      const type = t.decimal(10, 2);
      expect(type._tinybirdType).toBe("Decimal(10, 2)");
    });

    it("generates FixedString type", () => {
      const type = t.fixedString(3);
      expect(type._tinybirdType).toBe("FixedString(3)");
    });

    it("generates Tuple type", () => {
      const type = t.tuple(t.string(), t.int32());
      expect(type._tinybirdType).toBe("Tuple(String, Int32)");
    });

    it("generates DateTime64 type", () => {
      const type = t.dateTime64(3);
      expect(type._tinybirdType).toBe("DateTime64(3)");
    });

    it("generates DateTime64 with timezone", () => {
      const type = t.dateTime64(3, "UTC");
      expect(type._tinybirdType).toBe("DateTime64(3, 'UTC')");
    });
  });

  describe("Aggregate function types", () => {
    it("generates AggregateFunction with an explicit state type", () => {
      const type = t.aggregateFunction("uniq", t.string());
      expect(type._tinybirdType).toBe("AggregateFunction(uniq, String)");
      expectTypeOf(type._type).toEqualTypeOf<string>();
    });

    it("generates AggregateFunction without an explicit state type", () => {
      const type = t.aggregateFunction("count");
      expect(type._tinybirdType).toBe("AggregateFunction(count)");
      expectTypeOf(type._type).toEqualTypeOf<unknown>();
    });
  });

  describe("Helper functions", () => {
    it("isTypeValidator returns true for validators", () => {
      expect(isTypeValidator(t.string())).toBe(true);
    });

    it("isTypeValidator returns false for non-validators", () => {
      expect(isTypeValidator("string")).toBe(false);
      expect(isTypeValidator({})).toBe(false);
      expect(isTypeValidator(null)).toBe(false);
    });

    it("getTinybirdType returns type string", () => {
      expect(getTinybirdType(t.string())).toBe("String");
    });

    it("getModifiers returns modifiers object", () => {
      const modifiers = getModifiers(t.string().nullable());
      expect(modifiers.nullable).toBe(true);
    });
  });

  describe("Chained modifiers", () => {
    it("supports multiple modifiers", () => {
      const type = t.string().lowCardinality().default("test");
      expect(type._tinybirdType).toBe("LowCardinality(String)");
      expect(type._modifiers.lowCardinality).toBe(true);
      expect(type._modifiers.hasDefault).toBe(true);
      expect(type._modifiers.defaultValue).toBe("test");
    });
  });

  describe("Enum types", () => {
    it("generates Enum8 with value mapping", () => {
      const type = t.enum8("active", "inactive", "pending");
      expect(type._tinybirdType).toBe(
        "Enum8('active' = 1, 'inactive' = 2, 'pending' = 3)",
      );
    });

    it("generates Enum16 with value mapping", () => {
      const type = t.enum16("draft", "published", "archived");
      expect(type._tinybirdType).toBe(
        "Enum16('draft' = 1, 'published' = 2, 'archived' = 3)",
      );
    });

    it("escapes single quotes in enum values", () => {
      const type = t.enum8("it's ok", "normal");
      expect(type._tinybirdType).toBe("Enum8('it\\'s ok' = 1, 'normal' = 2)");
    });

    it("handles single enum value", () => {
      const type = t.enum8("only");
      expect(type._tinybirdType).toBe("Enum8('only' = 1)");
    });
  });

  describe("Custom type generics", () => {
    // Branded/nominal type helpers for testing
    type UserId = string & { readonly __brand: "UserId" };
    type TraceId = string & { readonly __brand: "TraceId" };
    type Timestamp = string & { readonly __brand: "Timestamp" };
    type Count = number & { readonly __brand: "Count" };
    type Price = number & { readonly __brand: "Price" };
    type BigId = bigint & { readonly __brand: "BigId" };
    type IsActive = boolean & { readonly __brand: "IsActive" };

    describe("runtime behavior unchanged", () => {
      it("string with generic produces same _tinybirdType", () => {
        expect(t.string<UserId>()._tinybirdType).toBe(t.string()._tinybirdType);
        expect(t.string<UserId>()._tinybirdType).toBe("String");
      });

      it("int32 with generic produces same _tinybirdType", () => {
        expect(t.int32<Count>()._tinybirdType).toBe(t.int32()._tinybirdType);
      });

      it("uuid with generic produces same _tinybirdType", () => {
        expect(t.uuid<TraceId>()._tinybirdType).toBe("UUID");
      });

      it("dateTime with generic produces same _tinybirdType", () => {
        expect(t.dateTime<Timestamp>()._tinybirdType).toBe("DateTime");
        expect(t.dateTime<Timestamp>("UTC")._tinybirdType).toBe(
          "DateTime('UTC')",
        );
      });

      it("bool with generic produces same _tinybirdType", () => {
        expect(t.bool<IsActive>()._tinybirdType).toBe("Bool");
      });

      it("int128 with generic produces same _tinybirdType", () => {
        expect(t.int128<BigId>()._tinybirdType).toBe("Int128");
      });

      it("decimal with generic produces same _tinybirdType", () => {
        expect(t.decimal<Price>(10, 2)._tinybirdType).toBe("Decimal(10, 2)");
      });

      it("fixedString with generic produces same _tinybirdType", () => {
        type CountryCode = string & { readonly __brand: "CountryCode" };
        expect(t.fixedString<CountryCode>(2)._tinybirdType).toBe(
          "FixedString(2)",
        );
      });
    });

    describe("modifiers work with custom generics", () => {
      it("nullable", () => {
        const v = t.string<UserId>().nullable();
        expect(v._tinybirdType).toBe("Nullable(String)");
        expect(v._modifiers.nullable).toBe(true);
      });

      it("lowCardinality", () => {
        expect(t.string<UserId>().lowCardinality()._tinybirdType).toBe(
          "LowCardinality(String)",
        );
      });

      it("default", () => {
        const v = t.string<UserId>().default("fallback" as UserId);
        expect(v._modifiers.hasDefault).toBe(true);
        expect(v._modifiers.defaultValue).toBe("fallback");
      });
    });

    describe("type inference", () => {
      it("validators without generics still infer base types", () => {
        expectTypeOf(t.string()._type).toEqualTypeOf<string>();
        expectTypeOf(t.int32()._type).toEqualTypeOf<number>();
        expectTypeOf(t.bool()._type).toEqualTypeOf<boolean>();
        expectTypeOf(t.int128()._type).toEqualTypeOf<bigint>();
        expectTypeOf(t.uuid()._type).toEqualTypeOf<string>();
      });

      it("validators with generics infer the custom type", () => {
        expectTypeOf(t.string<UserId>()._type).toEqualTypeOf<UserId>();
        expectTypeOf(t.uuid<TraceId>()._type).toEqualTypeOf<TraceId>();
        expectTypeOf(t.int32<Count>()._type).toEqualTypeOf<Count>();
        expectTypeOf(t.bool<IsActive>()._type).toEqualTypeOf<IsActive>();
        expectTypeOf(t.int128<BigId>()._type).toEqualTypeOf<BigId>();
        expectTypeOf(t.dateTime<Timestamp>()._type).toEqualTypeOf<Timestamp>();
        expectTypeOf(
          t.dateTime<Timestamp>("UTC")._type,
        ).toEqualTypeOf<Timestamp>();
        expectTypeOf(
          t.dateTime64<Timestamp>(3)._type,
        ).toEqualTypeOf<Timestamp>();
        expectTypeOf(t.decimal<Price>(10, 2)._type).toEqualTypeOf<Price>();
      });

      it("custom types flow through nullable", () => {
        expectTypeOf(
          t.string<UserId>().nullable()._type,
        ).toEqualTypeOf<UserId | null>();
        expectTypeOf(
          t.int32<Count>().nullable()._type,
        ).toEqualTypeOf<Count | null>();
      });

      it("custom types flow through lowCardinality", () => {
        expectTypeOf(
          t.string<UserId>().lowCardinality()._type,
        ).toEqualTypeOf<UserId>();
      });

      it("custom types flow through InferRow", () => {
        const ds = defineDatasource("test_custom_types", {
          schema: {
            user_id: t.string<UserId>(),
            event_count: t.int32<Count>(),
            created_at: t.dateTime<Timestamp>(),
            name: t.string(),
          },
          engine: engine.mergeTree({ sortingKey: ["user_id"] }),
        });

        type Row = InferRow<typeof ds>;

        expectTypeOf<Row["user_id"]>().toEqualTypeOf<UserId>();
        expectTypeOf<Row["event_count"]>().toEqualTypeOf<Count>();
        expectTypeOf<Row["created_at"]>().toEqualTypeOf<Timestamp>();
        expectTypeOf<Row["name"]>().toEqualTypeOf<string>();
      });

      it("rejects generics that violate base type constraint", () => {
        // @ts-expect-error - number does not extend string
        t.string<number>();

        // @ts-expect-error - string does not extend number
        t.int32<string>();

        // @ts-expect-error - string does not extend boolean
        t.bool<string>();

        // @ts-expect-error - number does not extend bigint
        t.int128<number>();
      });
    });

    describe("all validators accept custom generics", () => {
      it("string-based validators", () => {
        type S = string & { readonly __brand: "S" };
        expectTypeOf(t.string<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.fixedString<S>(10)._type).toEqualTypeOf<S>();
        expectTypeOf(t.uuid<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.ipv4<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.ipv6<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.date<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.date32<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.dateTime<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.dateTime<S>("UTC")._type).toEqualTypeOf<S>();
        expectTypeOf(t.dateTime64<S>()._type).toEqualTypeOf<S>();
        expectTypeOf(t.dateTime64<S>(6, "UTC")._type).toEqualTypeOf<S>();
      });

      it("number-based validators", () => {
        type N = number & { readonly __brand: "N" };
        expectTypeOf(t.int8<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.int16<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.int32<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.int64<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.uint8<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.uint16<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.uint32<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.uint64<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.float32<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.float64<N>()._type).toEqualTypeOf<N>();
        expectTypeOf(t.decimal<N>(10, 2)._type).toEqualTypeOf<N>();
      });

      it("bigint-based validators", () => {
        type B = bigint & { readonly __brand: "B" };
        expectTypeOf(t.int128<B>()._type).toEqualTypeOf<B>();
        expectTypeOf(t.int256<B>()._type).toEqualTypeOf<B>();
        expectTypeOf(t.uint128<B>()._type).toEqualTypeOf<B>();
        expectTypeOf(t.uint256<B>()._type).toEqualTypeOf<B>();
      });

      it("boolean-based validators", () => {
        type Bool = boolean & { readonly __brand: "Bool" };
        expectTypeOf(t.bool<Bool>()._type).toEqualTypeOf<Bool>();
      });
    });
  });
});
