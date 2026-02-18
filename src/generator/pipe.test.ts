import { describe, it, expect } from 'vitest';
import { generatePipe, generateAllPipes } from './pipe.js';
import { definePipe, defineMaterializedView, defineSinkPipe, node } from '../schema/pipe.js';
import { defineDatasource } from '../schema/datasource.js';
import { defineKafkaConnection, defineS3Connection } from '../schema/connection.js';
import { defineToken } from '../schema/token.js';
import { t } from '../schema/types.js';
import { p } from '../schema/params.js';
import { engine } from '../schema/engines.js';

// Helper to create a simple output schema for tests
const simpleOutput = { result: t.int32() };

describe('Pipe Generator', () => {
  describe('generatePipe', () => {
    it('generates basic pipe with node', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: 'SELECT * FROM table',
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.name).toBe('test_pipe');
      expect(result.content).toContain('NODE endpoint');
      expect(result.content).toContain('SQL >');
      expect(result.content).toContain('SELECT * FROM table');
    });

    it('includes description when provided', () => {
      const pipe = definePipe('test_pipe', {
        description: 'Test pipe description',
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Test pipe description');
    });

    it('includes TYPE endpoint when endpoint is true', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TYPE endpoint');
    });

    it('does not include TYPE endpoint when endpoint is false', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: false,
      });

      const result = generatePipe(pipe);
      expect(result.content).not.toContain('TYPE endpoint');
    });
  });

  describe('Dynamic SQL detection', () => {
    it('adds % on its own line for SQL with template parameters', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: 'SELECT * FROM table WHERE id = {{Int32(id)}}',
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('SQL >\n    %\n    SELECT');
    });

    it('adds % for SQL with DateTime parameter', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: 'SELECT * FROM table WHERE timestamp >= {{DateTime(start_date)}}',
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('    %\n');
    });

    it('does not add % for SQL without template parameters', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: 'SELECT * FROM table',
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).not.toContain('%');
    });

    it('does not add % for SQL with curly braces that are not parameters', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: "SELECT JSONExtract(data, 'field', 'String') FROM table",
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).not.toContain('%');
    });

    it('adds % for SQL with Jinja block syntax {%...%}', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: `SELECT * FROM table {% if defined(filter) %} WHERE id = {{Int32(filter)}} {% end %}`,
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('SQL >\n    %\n    SELECT');
    });

    it('adds % for SQL with only Jinja block syntax (no {{...}})', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            sql: `SELECT * FROM table {% if true %} LIMIT 10 {% end %}`,
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('    %\n');
    });

    it('injects param defaults into placeholders when SQL omits them', () => {
      const pipe = definePipe('defaults_pipe', {
        params: {
          start_date: p.date().optional('2025-03-01'),
          page: p.int32().optional(0),
        },
        nodes: [
          node({
            name: 'endpoint',
            sql: 'SELECT * FROM events WHERE d >= {{Date(start_date)}} LIMIT 10 OFFSET {{Int32(page)}}',
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain("{{ Date(start_date, '2025-03-01') }}");
      expect(result.content).toContain('{{ Int32(page, 0) }}');
    });
  });

  describe('Multiple nodes', () => {
    it('generates all nodes with separation', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({ name: 'first', sql: 'SELECT * FROM table1' }),
          node({ name: 'second', sql: 'SELECT * FROM first' }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('NODE first');
      expect(result.content).toContain('NODE second');
      expect(result.content).toContain('SELECT * FROM table1');
      expect(result.content).toContain('SELECT * FROM first');
    });

    it('includes node descriptions', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [
          node({
            name: 'endpoint',
            description: 'This is a test node',
            sql: 'SELECT 1',
          }),
        ],
        output: simpleOutput,
        endpoint: true,
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('NODE endpoint');
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('This is a test node');
    });
  });

  describe('Endpoint configuration', () => {
    it('includes cache when enabled', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: {
          enabled: true,
          cache: { enabled: true, ttl: 300 },
        },
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TYPE endpoint');
      expect(result.content).toContain('CACHE 300');
    });

    it('uses default cache TTL when not specified', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: {
          enabled: true,
          cache: { enabled: true },
        },
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('CACHE 60');
    });
  });

  describe('generateAllPipes', () => {
    it('generates all pipes', () => {
      const pipe1 = definePipe('pipe1', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
      });
      const pipe2 = definePipe('pipe2', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 2' })],
        output: simpleOutput,
        endpoint: true,
      });

      const results = generateAllPipes({ pipe1, pipe2 });
      expect(results).toHaveLength(2);
      expect(results.map(r => r.name).sort()).toEqual(['pipe1', 'pipe2']);
    });
  });

  describe('Full integration', () => {
    it('generates complete pipe file', () => {
      const pipe = definePipe('top_pages', {
        description: 'Get the most visited pages',
        params: {
          start_date: p.dateTime(),
          end_date: p.dateTime(),
          limit: p.int32().optional(10),
        },
        nodes: [
          node({
            name: 'aggregated',
            sql: `
SELECT
  pathname,
  count() AS views
FROM page_views
WHERE timestamp >= {{DateTime(start_date)}}
  AND timestamp <= {{DateTime(end_date)}}
GROUP BY pathname
ORDER BY views DESC
LIMIT {{Int32(limit, 10)}}
            `.trim(),
          }),
        ],
        output: {
          pathname: t.string(),
          views: t.uint64(),
        },
        endpoint: true,
      });

      const result = generatePipe(pipe);

      expect(result.name).toBe('top_pages');
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Get the most visited pages');
      expect(result.content).toContain('NODE aggregated');
      expect(result.content).toContain('SQL >');
      expect(result.content).toContain('    %\n');
      expect(result.content).toContain('pathname');
      expect(result.content).toContain('{{DateTime(start_date)}}');
      expect(result.content).toContain('{{Int32(limit, 10)}}');
      expect(result.content).toContain('TYPE endpoint');
    });
  });

  describe('Materialized Views', () => {
    const salesByHour = defineDatasource('sales_by_hour', {
      schema: {
        day: t.date(),
        country: t.string().lowCardinality(),
        total_sales: t.simpleAggregateFunction('sum', t.uint64()),
      },
      engine: engine.aggregatingMergeTree({
        sortingKey: ['day', 'country'],
      }),
    });

    it('generates TYPE MATERIALIZED and DATASOURCE with datasource', () => {
      const pipe = definePipe('sales_by_hour_mv', {
        nodes: [
          node({
            name: 'daily_sales',
            sql: 'SELECT toStartOfDay(date) as day, country, sum(sales) as total_sales FROM teams GROUP BY day, country',
          }),
        ],
        output: {
          day: t.date(),
          country: t.string().lowCardinality(),
          total_sales: t.simpleAggregateFunction('sum', t.uint64()),
        },
        materialized: {
          datasource: salesByHour,
        },
      });

      const result = generatePipe(pipe);

      expect(result.content).toContain('TYPE MATERIALIZED');
      expect(result.content).toContain('DATASOURCE sales_by_hour');
      expect(result.content).not.toContain('TYPE endpoint');
    });

    it('generates DEPLOYMENT_METHOD alter when specified', () => {
      const pipe = definePipe('sales_by_hour_mv', {
        nodes: [
          node({
            name: 'daily_sales',
            sql: 'SELECT toStartOfDay(date) as day, country, sum(sales) as total_sales FROM teams GROUP BY day, country',
          }),
        ],
        output: {
          day: t.date(),
          country: t.string().lowCardinality(),
          total_sales: t.simpleAggregateFunction('sum', t.uint64()),
        },
        materialized: {
          datasource: salesByHour,
          deploymentMethod: 'alter',
        },
      });

      const result = generatePipe(pipe);

      expect(result.content).toContain('TYPE MATERIALIZED');
      expect(result.content).toContain('DATASOURCE sales_by_hour');
      expect(result.content).toContain('DEPLOYMENT_METHOD alter');
    });

    it('does not include DEPLOYMENT_METHOD when not specified', () => {
      const pipe = definePipe('sales_by_hour_mv', {
        nodes: [
          node({
            name: 'daily_sales',
            sql: 'SELECT toStartOfDay(date) as day, country, sum(sales) as total_sales FROM teams GROUP BY day, country',
          }),
        ],
        output: {
          day: t.date(),
          country: t.string().lowCardinality(),
          total_sales: t.simpleAggregateFunction('sum', t.uint64()),
        },
        materialized: {
          datasource: salesByHour,
        },
      });

      const result = generatePipe(pipe);

      expect(result.content).not.toContain('DEPLOYMENT_METHOD');
    });

    it('generates complete materialized view pipe file', () => {
      const pipe = definePipe('sales_by_hour_mv', {
        description: 'Aggregate sales per hour',
        nodes: [
          node({
            name: 'daily_sales',
            sql: `
SELECT
  toStartOfDay(starting_date) as day,
  country,
  sum(sales) as total_sales
FROM teams
GROUP BY day, country
            `.trim(),
          }),
        ],
        output: {
          day: t.date(),
          country: t.string().lowCardinality(),
          total_sales: t.simpleAggregateFunction('sum', t.uint64()),
        },
        materialized: {
          datasource: salesByHour,
          deploymentMethod: 'alter',
        },
      });

      const result = generatePipe(pipe);

      expect(result.name).toBe('sales_by_hour_mv');
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Aggregate sales per hour');
      expect(result.content).toContain('NODE daily_sales');
      expect(result.content).toContain('SQL >');
      expect(result.content).toContain('toStartOfDay(starting_date) as day');
      expect(result.content).toContain('TYPE MATERIALIZED');
      expect(result.content).toContain('DATASOURCE sales_by_hour');
      expect(result.content).toContain('DEPLOYMENT_METHOD alter');
    });

    it('works with defineMaterializedView helper using datasource', () => {
      const pipe = defineMaterializedView('sales_mv', {
        description: 'Sales materialized view',
        datasource: salesByHour,
        nodes: [
          node({
            name: 'daily_sales',
            sql: 'SELECT toStartOfDay(date) as day, country, sum(sales) as total_sales FROM events GROUP BY day, country',
          }),
        ],
        deploymentMethod: 'alter',
      });

      const result = generatePipe(pipe);

      expect(result.name).toBe('sales_mv');
      expect(result.content).toContain('TYPE MATERIALIZED');
      expect(result.content).toContain('DATASOURCE sales_by_hour');
      expect(result.content).toContain('DEPLOYMENT_METHOD alter');
    });

    it('generates DATASOURCE correctly with datasource field', () => {
      const pipe = definePipe('sales_by_hour_mv_2', {
        nodes: [
          node({
            name: 'daily_sales',
            sql: 'SELECT toStartOfDay(date) as day, country, sum(sales) as total_sales FROM teams GROUP BY day, country',
          }),
        ],
        output: {
          day: t.date(),
          country: t.string().lowCardinality(),
          total_sales: t.simpleAggregateFunction('sum', t.uint64()),
        },
        materialized: {
          datasource: salesByHour,
        },
      });

      const result = generatePipe(pipe);

      expect(result.content).toContain('TYPE MATERIALIZED');
      expect(result.content).toContain('DATASOURCE sales_by_hour');
    });
  });

  describe('Sink configuration', () => {
    it('generates Kafka sink directives', () => {
      const kafka = defineKafkaConnection('events_kafka', {
        bootstrapServers: 'localhost:9092',
      });

      const pipe = defineSinkPipe('events_sink', {
        nodes: [node({ name: 'publish', sql: 'SELECT * FROM events' })],
        sink: {
          connection: kafka,
          topic: 'events_out',
          schedule: '@on-demand',
        },
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TYPE sink');
      expect(result.content).toContain('EXPORT_CONNECTION_NAME events_kafka');
      expect(result.content).toContain('EXPORT_KAFKA_TOPIC events_out');
      expect(result.content).toContain('EXPORT_SCHEDULE @on-demand');
      expect(result.content).not.toContain('EXPORT_STRATEGY');
    });

    it('generates S3 sink directives', () => {
      const s3 = defineS3Connection('exports_s3', {
        region: 'us-east-1',
        arn: 'arn:aws:iam::123456789012:role/tinybird-s3-access',
      });

      const pipe = defineSinkPipe('events_s3_sink', {
        nodes: [node({ name: 'export', sql: 'SELECT * FROM events' })],
        sink: {
          connection: s3,
          bucketUri: 's3://bucket/events/',
          fileTemplate: 'events_{date}',
          format: 'csv',
          schedule: '@once',
          compression: 'gzip',
          strategy: 'replace',
        },
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TYPE sink');
      expect(result.content).toContain('EXPORT_CONNECTION_NAME exports_s3');
      expect(result.content).toContain('EXPORT_BUCKET_URI s3://bucket/events/');
      expect(result.content).toContain('EXPORT_FILE_TEMPLATE events_{date}');
      expect(result.content).toContain('EXPORT_FORMAT csv');
      expect(result.content).toContain('EXPORT_SCHEDULE @once');
      expect(result.content).toContain('EXPORT_STRATEGY replace');
      expect(result.content).toContain('EXPORT_COMPRESSION gzip');
    });
  });

  describe('Token generation', () => {
    it('generates TOKEN lines with inline config', () => {
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
        tokens: [{ name: 'app_read' }],
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TOKEN app_read READ');
    });

    it('generates TOKEN lines with token reference', () => {
      const appToken = defineToken('my_token');
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
        tokens: [{ token: appToken, scope: 'READ' }],
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TOKEN my_token READ');
    });

    it('generates multiple TOKEN lines for multiple tokens', () => {
      const token1 = defineToken('token_one');
      const token2 = defineToken('token_two');
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
        tokens: [
          { token: token1, scope: 'READ' },
          { token: token2, scope: 'READ' },
        ],
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TOKEN token_one READ');
      expect(result.content).toContain('TOKEN token_two READ');
    });

    it('generates mixed inline and reference tokens', () => {
      const refToken = defineToken('ref_token');
      const pipe = definePipe('test_pipe', {
        nodes: [node({ name: 'endpoint', sql: 'SELECT 1' })],
        output: simpleOutput,
        endpoint: true,
        tokens: [
          { name: 'inline_token' },
          { token: refToken, scope: 'READ' },
        ],
      });

      const result = generatePipe(pipe);
      expect(result.content).toContain('TOKEN inline_token READ');
      expect(result.content).toContain('TOKEN ref_token READ');
    });
  });
});
