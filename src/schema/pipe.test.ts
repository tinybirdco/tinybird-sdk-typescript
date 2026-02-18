import { describe, it, expect } from "vitest";
import {
  definePipe,
  defineSinkPipe,
  defineMaterializedView,
  node,
  isPipeDefinition,
  getEndpointConfig,
  getMaterializedConfig,
  getSinkConfig,
  isMaterializedView,
  isSinkPipe,
  getNodeNames,
  getNode,
  sql,
} from "./pipe.js";
import { defineDatasource } from "./datasource.js";
import { defineKafkaConnection, defineS3Connection } from "./connection.js";
import { t } from "./types.js";
import { p } from "./params.js";
import { engine } from "./engines.js";

describe("Pipe Schema", () => {
  describe("node", () => {
    it("creates a node with required fields", () => {
      const n = node({
        name: "endpoint",
        sql: "SELECT * FROM events",
      });

      expect(n._name).toBe("endpoint");
      expect(n.sql).toBe("SELECT * FROM events");
    });

    it("creates a node with description", () => {
      const n = node({
        name: "endpoint",
        description: "Main query node",
        sql: "SELECT * FROM events",
      });

      expect(n.description).toBe("Main query node");
    });
  });

  describe("definePipe", () => {
    it("creates a pipe with required fields", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(pipe._name).toBe("my_pipe");
      expect(pipe._type).toBe("pipe");
      expect(pipe.options.nodes).toHaveLength(1);
    });

    it("creates a pipe with params", () => {
      const pipe = definePipe("my_pipe", {
        params: {
          start_date: p.dateTime(),
          limit: p.int32().optional(10),
        },
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(pipe._params).toBeDefined();
      expect(pipe.options.params).toBeDefined();
    });

    it("creates a pipe with description", () => {
      const pipe = definePipe("my_pipe", {
        description: "A test pipe",
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(pipe.options.description).toBe("A test pipe");
    });

    it("throws error for invalid pipe name", () => {
      expect(() =>
        definePipe("123invalid", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: { value: t.int32() },
          endpoint: true,
        })
      ).toThrow("Invalid pipe name");

      expect(() =>
        definePipe("my-pipe", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: { value: t.int32() },
          endpoint: true,
        })
      ).toThrow("Invalid pipe name");
    });

    it("throws error for empty nodes", () => {
      expect(() =>
        definePipe("my_pipe", {
          nodes: [],
          output: { value: t.int32() },
          endpoint: true,
        })
      ).toThrow("must have at least one node");
    });

    it("throws error for empty output", () => {
      expect(() =>
        definePipe("my_pipe", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: {},
          endpoint: true,
        })
      ).toThrow("must have an output schema");
    });

    it("allows valid naming patterns", () => {
      const pipe1 = definePipe("_private_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });
      expect(pipe1._name).toBe("_private_pipe");

      const pipe2 = definePipe("pipe_v2", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });
      expect(pipe2._name).toBe("pipe_v2");
    });
  });

  describe("isPipeDefinition", () => {
    it("returns true for valid pipe", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(isPipeDefinition(pipe)).toBe(true);
    });

    it("returns false for non-pipe objects", () => {
      expect(isPipeDefinition({})).toBe(false);
      expect(isPipeDefinition(null)).toBe(false);
      expect(isPipeDefinition(undefined)).toBe(false);
      expect(isPipeDefinition("string")).toBe(false);
      expect(isPipeDefinition(123)).toBe(false);
      expect(isPipeDefinition({ _name: "test" })).toBe(false);
    });
  });

  describe("Sink pipes", () => {
    it("creates a Kafka sink pipe", () => {
      const kafka = defineKafkaConnection("events_kafka", {
        bootstrapServers: "localhost:9092",
      });

      const pipe = defineSinkPipe("events_sink", {
        nodes: [node({ name: "publish", sql: "SELECT * FROM events" })],
        sink: {
          connection: kafka,
          topic: "events_out",
          schedule: "@on-demand",
        },
      });

      const sink = getSinkConfig(pipe);
      expect(sink).toBeTruthy();
      expect(sink && "topic" in sink ? sink.topic : undefined).toBe("events_out");
      expect(isSinkPipe(pipe)).toBe(true);
    });

    it("creates an S3 sink pipe", () => {
      const s3 = defineS3Connection("exports_s3", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      const pipe = defineSinkPipe("exports_sink", {
        nodes: [node({ name: "export", sql: "SELECT * FROM events" })],
        sink: {
          connection: s3,
          bucketUri: "s3://exports/events/",
          fileTemplate: "events_{date}",
          schedule: "@once",
          format: "csv",
          strategy: "create_new",
          compression: "gzip",
        },
      });

      const sink = getSinkConfig(pipe);
      expect(sink).toBeTruthy();
      expect(sink && "bucketUri" in sink ? sink.bucketUri : undefined).toBe("s3://exports/events/");
      expect(isSinkPipe(pipe)).toBe(true);
    });

    it("throws when Kafka sink connection type is invalid", () => {
      const s3 = defineS3Connection("exports_s3", {
        region: "us-east-1",
        arn: "arn:aws:iam::123456789012:role/tinybird-s3-access",
      });

      expect(() =>
        defineSinkPipe("bad_sink", {
          nodes: [node({ name: "export", sql: "SELECT * FROM events" })],
          sink: {
            // Runtime validation rejects mismatched connection/type
            connection: s3 as unknown as ReturnType<typeof defineKafkaConnection>,
            topic: "events_out",
            schedule: "@on-demand",
          },
        })
      ).toThrow("requires a Kafka connection");
    });

    it("throws when sink configuration is passed to definePipe", () => {
      const kafka = defineKafkaConnection("events_kafka", {
        bootstrapServers: "localhost:9092",
      });

      expect(() =>
        definePipe(
          "bad_via_define_pipe",
          {
            nodes: [node({ name: "export", sql: "SELECT * FROM events" })],
            sink: {
              connection: kafka,
              topic: "events_out",
            },
          } as unknown as Parameters<typeof definePipe>[1]
        )
      ).toThrow("must be created with defineSinkPipe");
    });
  });

  describe("getEndpointConfig", () => {
    it("returns null when endpoint is false", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: false,
      });

      expect(getEndpointConfig(pipe)).toBeNull();
    });

    it("returns config when endpoint is true", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      const config = getEndpointConfig(pipe);
      expect(config).toEqual({ enabled: true });
    });

    it("returns config with cache settings", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: {
          enabled: true,
          cache: { enabled: true, ttl: 300 },
        },
      });

      const config = getEndpointConfig(pipe);
      expect(config?.enabled).toBe(true);
      expect(config?.cache?.ttl).toBe(300);
    });

    it("returns null when endpoint config has enabled: false", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: {
          enabled: false,
        },
      });

      expect(getEndpointConfig(pipe)).toBeNull();
    });
  });

  describe("getNodeNames", () => {
    it("returns all node names", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [
          node({ name: "first", sql: "SELECT 1" }),
          node({ name: "second", sql: "SELECT 2" }),
          node({ name: "endpoint", sql: "SELECT 3" }),
        ],
        output: { value: t.int32() },
        endpoint: true,
      });

      const names = getNodeNames(pipe);
      expect(names).toEqual(["first", "second", "endpoint"]);
    });
  });

  describe("getNode", () => {
    it("returns node by name", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [
          node({ name: "first", sql: "SELECT 1" }),
          node({ name: "endpoint", sql: "SELECT 2" }),
        ],
        output: { value: t.int32() },
        endpoint: true,
      });

      const n = getNode(pipe, "first");
      expect(n?._name).toBe("first");
      expect(n?.sql).toBe("SELECT 1");
    });

    it("returns undefined for non-existent node", () => {
      const pipe = definePipe("my_pipe", {
        nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
        output: { value: t.int32() },
        endpoint: true,
      });

      expect(getNode(pipe, "nonexistent")).toBeUndefined();
    });
  });

  describe("sql template helper", () => {
    it("interpolates datasource references", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });

      const query = sql`SELECT * FROM ${events}`;
      expect(query).toBe("SELECT * FROM events");
    });

    it("interpolates node references", () => {
      const n = node({ name: "aggregated", sql: "SELECT 1" });

      const query = sql`SELECT * FROM ${n}`;
      expect(query).toBe("SELECT * FROM aggregated");
    });

    it("interpolates string values", () => {
      const tableName = "events";
      const query = sql`SELECT * FROM ${tableName}`;
      expect(query).toBe("SELECT * FROM events");
    });

    it("interpolates number values", () => {
      const limit = 10;
      const query = sql`SELECT * FROM events LIMIT ${limit}`;
      expect(query).toBe("SELECT * FROM events LIMIT 10");
    });

    it("handles multiple interpolations", () => {
      const events = defineDatasource("events", {
        schema: { id: t.string() },
      });
      const limit = 100;

      const query = sql`SELECT * FROM ${events} WHERE id = ${"test"} LIMIT ${limit}`;
      expect(query).toBe("SELECT * FROM events WHERE id = test LIMIT 100");
    });

    it("handles no interpolations", () => {
      const query = sql`SELECT 1`;
      expect(query).toBe("SELECT 1");
    });
  });

  describe("Materialized Views", () => {
    const salesByHour = defineDatasource("sales_by_hour", {
      schema: {
        day: t.date(),
        country: t.string().lowCardinality(),
        total_sales: t.simpleAggregateFunction("sum", t.uint64()),
      },
      engine: engine.aggregatingMergeTree({
        sortingKey: ["day", "country"],
      }),
    });

    describe("definePipe with materialized", () => {
      it("creates a materialized view pipe with datasource (preferred)", () => {
        const pipe = definePipe("sales_by_hour_mv", {
          description: "Aggregate sales per hour",
          nodes: [
            node({
              name: "daily_sales",
              sql: `
                SELECT
                  toStartOfDay(starting_date) as day,
                  country,
                  sum(sales) as total_sales
                FROM teams
                GROUP BY day, country
              `,
            }),
          ],
          output: {
            day: t.date(),
            country: t.string().lowCardinality(),
            total_sales: t.simpleAggregateFunction("sum", t.uint64()),
          },
          materialized: {
            datasource: salesByHour,
          },
        });

        expect(pipe._name).toBe("sales_by_hour_mv");
        expect(pipe.options.materialized).toBeDefined();
        // Internally normalized to datasource
        expect(pipe.options.materialized?.datasource?._name).toBe("sales_by_hour");
      });

      it("creates a materialized view pipe with datasource", () => {
        const pipe = definePipe("sales_by_hour_mv_2", {
          nodes: [node({ name: "mv", sql: "SELECT 1 as day, 'US' as country, 100 as total_sales" })],
          output: {
            day: t.date(),
            country: t.string().lowCardinality(),
            total_sales: t.simpleAggregateFunction("sum", t.uint64()),
          },
          materialized: {
            datasource: salesByHour,
          },
        });

        expect(pipe._name).toBe("sales_by_hour_mv_2");
        expect(pipe.options.materialized).toBeDefined();
        expect(pipe.options.materialized?.datasource?._name).toBe("sales_by_hour");
      });

      it("creates a materialized view with deployment method", () => {
        const pipe = definePipe("sales_by_hour_mv", {
          nodes: [node({ name: "mv", sql: "SELECT 1 as day, 'US' as country, 100 as total_sales" })],
          output: {
            day: t.date(),
            country: t.string().lowCardinality(),
            total_sales: t.simpleAggregateFunction("sum", t.uint64()),
          },
          materialized: {
            datasource: salesByHour,
            deploymentMethod: "alter",
          },
        });

        expect(pipe.options.materialized?.deploymentMethod).toBe("alter");
      });

      it("throws error when both endpoint and materialized are set", () => {
        expect(() =>
          definePipe("invalid_pipe", {
            nodes: [node({ name: "endpoint", sql: "SELECT 1 as day, 'US' as country, 100 as total_sales" })],
            output: {
              day: t.date(),
              country: t.string().lowCardinality(),
              total_sales: t.simpleAggregateFunction("sum", t.uint64()),
            },
            endpoint: true,
            materialized: {
              datasource: salesByHour,
            },
          })
        ).toThrow("can only have one of: endpoint, materialized, or copy");
      });

    });

    describe("Schema validation", () => {
      it("throws error when output is missing columns", () => {
        expect(() =>
          definePipe("invalid_mv", {
            nodes: [node({ name: "mv", sql: "SELECT 1" })],
            output: {
              day: t.date(),
              // missing country and total_sales
            },
            materialized: {
              datasource: salesByHour,
            },
          })
        ).toThrow("missing columns from target datasource");
      });

      it("throws error when output has extra columns", () => {
        expect(() =>
          definePipe("invalid_mv", {
            nodes: [node({ name: "mv", sql: "SELECT 1" })],
            output: {
              day: t.date(),
              country: t.string().lowCardinality(),
              total_sales: t.simpleAggregateFunction("sum", t.uint64()),
              extra_column: t.string(), // extra column
            },
            materialized: {
              datasource: salesByHour,
            },
          })
        ).toThrow("columns not in target datasource");
      });

      it("throws error when column types do not match", () => {
        expect(() =>
          definePipe("invalid_mv", {
            nodes: [node({ name: "mv", sql: "SELECT 1" })],
            output: {
              day: t.string(), // should be date
              country: t.string().lowCardinality(),
              total_sales: t.simpleAggregateFunction("sum", t.uint64()),
            },
            materialized: {
              datasource: salesByHour,
            },
          })
        ).toThrow("type mismatch");
      });

      it("allows compatible types (base type to aggregate function)", () => {
        // When the output has UInt64 and datasource has SimpleAggregateFunction(sum, UInt64)
        // they should be compatible
        const simpleDatasource = defineDatasource("simple_agg", {
          schema: {
            value: t.simpleAggregateFunction("sum", t.uint64()),
          },
        });

        const pipe = definePipe("valid_mv", {
          nodes: [node({ name: "mv", sql: "SELECT sum(x) as value FROM table" })],
          output: {
            value: t.uint64(), // base type compatible with SimpleAggregateFunction(sum, UInt64)
          },
          materialized: {
            datasource: simpleDatasource,
          },
        });

        expect(pipe.options.materialized).toBeDefined();
      });

      it("allows compatible types with modifiers (nullable, low cardinality)", () => {
        const datasource = defineDatasource("test_ds", {
          schema: {
            name: t.string().lowCardinality().nullable(),
          },
        });

        const pipe = definePipe("valid_mv", {
          nodes: [node({ name: "mv", sql: "SELECT name FROM table" })],
          output: {
            name: t.string().lowCardinality().nullable(),
          },
          materialized: {
            datasource: datasource,
          },
        });

        expect(pipe.options.materialized).toBeDefined();
      });
    });

    describe("getMaterializedConfig", () => {
      it("returns null for endpoint pipe", () => {
        const pipe = definePipe("endpoint_pipe", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: { value: t.int32() },
          endpoint: true,
        });

        expect(getMaterializedConfig(pipe)).toBeNull();
      });

      it("returns config for materialized view with datasource", () => {
        const pipe = definePipe("mv_pipe", {
          nodes: [node({ name: "mv", sql: "SELECT 1 as day, 'US' as country, 100 as total_sales" })],
          output: {
            day: t.date(),
            country: t.string().lowCardinality(),
            total_sales: t.simpleAggregateFunction("sum", t.uint64()),
          },
          materialized: {
            datasource: salesByHour,
            deploymentMethod: "alter",
          },
        });

        const config = getMaterializedConfig(pipe);
        expect(config).toBeDefined();
        // Normalized config always has datasource set
        expect(config?.datasource?._name).toBe("sales_by_hour");
        expect(config?.deploymentMethod).toBe("alter");
      });
    });

    describe("isMaterializedView", () => {
      it("returns false for endpoint pipe", () => {
        const pipe = definePipe("endpoint_pipe", {
          nodes: [node({ name: "endpoint", sql: "SELECT 1" })],
          output: { value: t.int32() },
          endpoint: true,
        });

        expect(isMaterializedView(pipe)).toBe(false);
      });

      it("returns true for materialized view", () => {
        const pipe = definePipe("mv_pipe", {
          nodes: [node({ name: "mv", sql: "SELECT 1 as day, 'US' as country, 100 as total_sales" })],
          output: {
            day: t.date(),
            country: t.string().lowCardinality(),
            total_sales: t.simpleAggregateFunction("sum", t.uint64()),
          },
          materialized: {
            datasource: salesByHour,
          },
        });

        expect(isMaterializedView(pipe)).toBe(true);
      });
    });

    describe("defineMaterializedView", () => {
      it("creates a materialized view with inferred output schema using datasource", () => {
        const pipe = defineMaterializedView("sales_mv", {
          description: "Sales materialized view",
          datasource: salesByHour,
          nodes: [
            node({
              name: "daily_sales",
              sql: "SELECT toStartOfDay(date) as day, country, sum(sales) as total_sales FROM events GROUP BY day, country",
            }),
          ],
        });

        expect(pipe._name).toBe("sales_mv");
        expect(pipe.options.description).toBe("Sales materialized view");
        expect(pipe.options.materialized?.datasource?._name).toBe("sales_by_hour");
        expect(Object.keys(pipe._output!)).toEqual(["day", "country", "total_sales"]);
      });

      it("creates a materialized view with deployment method", () => {
        const pipe = defineMaterializedView("sales_mv", {
          datasource: salesByHour,
          nodes: [node({ name: "mv", sql: "SELECT 1" })],
          deploymentMethod: "alter",
        });

        expect(pipe.options.materialized?.deploymentMethod).toBe("alter");
      });

      it("creates a materialized view with datasource", () => {
        const pipe = defineMaterializedView("sales_mv_2", {
          datasource: salesByHour,
          nodes: [node({ name: "mv", sql: "SELECT 1" })],
        });

        expect(pipe._name).toBe("sales_mv_2");
        expect(pipe.options.materialized?.datasource?._name).toBe("sales_by_hour");
      });
    });
  });
});
