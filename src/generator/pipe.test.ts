import { describe, it, expect } from 'vitest';
import { generatePipe, generateAllPipes } from './pipe.js';
import { definePipe, defineMaterializedView, node } from '../schema/pipe.js';
import { defineDatasource } from '../schema/datasource.js';
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

    it('generates TYPE MATERIALIZED and DATASOURCE', () => {
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

    it('works with defineMaterializedView helper', () => {
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
  });
});
