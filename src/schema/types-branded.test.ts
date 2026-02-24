import { describe, it, expectTypeOf } from "vitest";
import { t } from "./types.js";
import { defineDatasource } from "./datasource.js";
import { engine } from "./engines.js";
import type { InferRow } from "../infer/index.js";

// ============ Branded type definitions for testing ============

type UserId = string & { readonly __brand: "UserId" };
type TraceId = string & { readonly __brand: "TraceId" };
type Timestamp = string & { readonly __brand: "Timestamp" };
type CountryCode = string & { readonly __brand: "CountryCode" };
type IpAddress = string & { readonly __brand: "IpAddress" };
type Count = number & { readonly __brand: "Count" };
type Price = number & { readonly __brand: "Price" };
type BigId = bigint & { readonly __brand: "BigId" };
type IsActive = boolean & { readonly __brand: "IsActive" };

describe("Type-level tests for branded type generics", () => {
  describe("backwards compatibility - validators without generics infer base types", () => {
    it("string() infers string", () => {
      const v = t.string();
      expectTypeOf(v._type).toEqualTypeOf<string>();
    });

    it("int32() infers number", () => {
      const v = t.int32();
      expectTypeOf(v._type).toEqualTypeOf<number>();
    });

    it("bool() infers boolean", () => {
      const v = t.bool();
      expectTypeOf(v._type).toEqualTypeOf<boolean>();
    });

    it("int128() infers bigint", () => {
      const v = t.int128();
      expectTypeOf(v._type).toEqualTypeOf<bigint>();
    });

    it("uuid() infers string", () => {
      const v = t.uuid();
      expectTypeOf(v._type).toEqualTypeOf<string>();
    });
  });

  describe("branded generics - validators with generics infer branded types", () => {
    it("string<UserId>() infers UserId", () => {
      const v = t.string<UserId>();
      expectTypeOf(v._type).toEqualTypeOf<UserId>();
    });

    it("uuid<TraceId>() infers TraceId", () => {
      const v = t.uuid<TraceId>();
      expectTypeOf(v._type).toEqualTypeOf<TraceId>();
    });

    it("int32<Count>() infers Count", () => {
      const v = t.int32<Count>();
      expectTypeOf(v._type).toEqualTypeOf<Count>();
    });

    it("bool<IsActive>() infers IsActive", () => {
      const v = t.bool<IsActive>();
      expectTypeOf(v._type).toEqualTypeOf<IsActive>();
    });

    it("int128<BigId>() infers BigId", () => {
      const v = t.int128<BigId>();
      expectTypeOf(v._type).toEqualTypeOf<BigId>();
    });

    it("dateTime<Timestamp>() infers Timestamp", () => {
      const v = t.dateTime<Timestamp>();
      expectTypeOf(v._type).toEqualTypeOf<Timestamp>();
    });

    it("dateTime<Timestamp>(timezone) infers Timestamp", () => {
      const v = t.dateTime<Timestamp>("UTC");
      expectTypeOf(v._type).toEqualTypeOf<Timestamp>();
    });

    it("dateTime64<Timestamp>() infers Timestamp", () => {
      const v = t.dateTime64<Timestamp>(3);
      expectTypeOf(v._type).toEqualTypeOf<Timestamp>();
    });

    it("decimal<Price>() infers Price", () => {
      const v = t.decimal<Price>(10, 2);
      expectTypeOf(v._type).toEqualTypeOf<Price>();
    });

    it("fixedString<CountryCode>() infers CountryCode", () => {
      const v = t.fixedString<CountryCode>(2);
      expectTypeOf(v._type).toEqualTypeOf<CountryCode>();
    });

    it("ipv4<IpAddress>() infers IpAddress", () => {
      const v = t.ipv4<IpAddress>();
      expectTypeOf(v._type).toEqualTypeOf<IpAddress>();
    });
  });

  describe("branded types flow through nullable modifier", () => {
    it("string<UserId>().nullable() infers UserId | null", () => {
      const v = t.string<UserId>().nullable();
      expectTypeOf(v._type).toEqualTypeOf<UserId | null>();
    });

    it("int32<Count>().nullable() infers Count | null", () => {
      const v = t.int32<Count>().nullable();
      expectTypeOf(v._type).toEqualTypeOf<Count | null>();
    });
  });

  describe("branded types flow through lowCardinality modifier", () => {
    it("string<UserId>().lowCardinality() still infers UserId", () => {
      const v = t.string<UserId>().lowCardinality();
      expectTypeOf(v._type).toEqualTypeOf<UserId>();
    });
  });

  describe("branded types flow through InferRow", () => {
    it("InferRow picks up branded types from schema", () => {
      const ds = defineDatasource("test_branded", {
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
  });

  describe("type constraint enforcement", () => {
    it("prevents using wrong base type", () => {
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

  describe("complete coverage - all validators accept generics", () => {
    it("all string-based validators accept string brands", () => {
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

    it("all number-based validators accept number brands", () => {
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

    it("all bigint-based validators accept bigint brands", () => {
      type B = bigint & { readonly __brand: "B" };
      expectTypeOf(t.int128<B>()._type).toEqualTypeOf<B>();
      expectTypeOf(t.int256<B>()._type).toEqualTypeOf<B>();
      expectTypeOf(t.uint128<B>()._type).toEqualTypeOf<B>();
      expectTypeOf(t.uint256<B>()._type).toEqualTypeOf<B>();
    });

    it("bool accepts boolean brands", () => {
      type Bool = boolean & { readonly __brand: "Bool" };
      expectTypeOf(t.bool<Bool>()._type).toEqualTypeOf<Bool>();
    });
  });
});
