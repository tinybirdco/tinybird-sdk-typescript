import { describe, it, expect } from 'vitest';
import { generateDatasource, generateAllDatasources } from './datasource.js';
import { defineDatasource } from '../schema/datasource.js';
import { createKafkaConnection } from '../schema/connection.js';
import { t } from '../schema/types.js';
import { engine } from '../schema/engines.js';

describe('Datasource Generator', () => {
  describe('generateDatasource', () => {
    it('generates basic datasource with schema', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          count: t.int32(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.name).toBe('test_ds');
      expect(result.content).toContain('SCHEMA >');
      expect(result.content).toContain('id String');
      expect(result.content).toContain('count Int32');
    });

    it('includes description when provided', () => {
      const ds = defineDatasource('test_ds', {
        description: 'Test datasource description',
        schema: {
          id: t.string(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Test datasource description');
    });

    it('includes engine configuration', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
        },
        engine: engine.mergeTree({
          sortingKey: ['id'],
        }),
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('ENGINE "MergeTree"');
      expect(result.content).toContain('ENGINE_SORTING_KEY "id"');
    });

    it('includes partition key in engine config', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          timestamp: t.dateTime(),
        },
        engine: engine.mergeTree({
          sortingKey: ['id'],
          partitionKey: 'toYYYYMM(timestamp)',
        }),
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
    });

    it('includes TTL in engine config', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          timestamp: t.dateTime(),
        },
        engine: engine.mergeTree({
          sortingKey: ['id'],
          ttl: 'timestamp + INTERVAL 90 DAY',
        }),
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('ENGINE_TTL "timestamp + INTERVAL 90 DAY"');
    });
  });

  describe('Column formatting', () => {
    it('formats Nullable columns correctly', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          name: t.string().nullable(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('name Nullable(String)');
    });

    it('formats LowCardinality columns correctly', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          country: t.string().lowCardinality(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('country LowCardinality(String)');
    });

    it('formats LowCardinality(Nullable) correctly', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          country: t.string().lowCardinality().nullable(),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('country LowCardinality(Nullable(String))');
    });

    it('includes default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          status: t.string().default('pending'),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain("status String `json:$.status` DEFAULT 'pending'");
    });

    it('formats null default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          // Using nullable with explicit null default
          status: t.string().nullable().default(null),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('DEFAULT NULL');
    });

    it('formats number default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          count: t.int32().default(42),
          score: t.float64().default(3.14),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('count Int32 `json:$.count` DEFAULT 42');
      expect(result.content).toContain('score Float64 `json:$.score` DEFAULT 3.14');
    });

    it('formats boolean default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          is_active: t.bool().default(true),
          is_deleted: t.bool().default(false),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('is_active Bool `json:$.is_active` DEFAULT 1');
      expect(result.content).toContain('is_deleted Bool `json:$.is_deleted` DEFAULT 0');
    });

    it('formats Date default values for DateTime type', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          created_at: t.dateTime().default(new Date('2024-01-15T10:30:00Z')),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain("created_at DateTime `json:$.created_at` DEFAULT '2024-01-15 10:30:00'");
    });

    it('formats Date default values for Date type', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          birth_date: t.date().default(new Date('2024-01-15T10:30:00Z')),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain("birth_date Date `json:$.birth_date` DEFAULT '2024-01-15'");
    });

    it('formats array default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          tags: t.array(t.string()).default(['a', 'b']),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('tags Array(String) `json:$.tags` DEFAULT ["a","b"]');
    });

    it('formats object default values for JSON type', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          metadata: t.json<{ key: string }>().default({ key: 'value' }),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('metadata JSON `json:$.metadata` DEFAULT {"key":"value"}');
    });

    it('escapes single quotes in string default values', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          message: t.string().default("it's working"),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain("message String `json:$.message` DEFAULT 'it\\'s working'");
    });

    it('includes codec', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          data: t.string().codec('LZ4'),
        },
      });

      const result = generateDatasource(ds);
      expect(result.content).toContain('data String `json:$.data` CODEC(LZ4)');
    });

    it('adds commas between columns except last', () => {
      const ds = defineDatasource('test_ds', {
        schema: {
          id: t.string(),
          name: t.string(),
          count: t.int32(),
        },
      });

      const result = generateDatasource(ds);
      const lines = result.content.split('\n');
      const schemaLines = lines.filter(l => l.trim().startsWith('id') || l.trim().startsWith('name') || l.trim().startsWith('count'));

      expect(schemaLines[0]).toContain(',');
      expect(schemaLines[1]).toContain(',');
      expect(schemaLines[2]).not.toContain(',');
    });
  });

  describe('generateAllDatasources', () => {
    it('generates all datasources', () => {
      const ds1 = defineDatasource('ds1', { schema: { id: t.string() } });
      const ds2 = defineDatasource('ds2', { schema: { id: t.int32() } });

      const results = generateAllDatasources({ ds1, ds2 });
      expect(results).toHaveLength(2);
      expect(results.map(r => r.name).sort()).toEqual(['ds1', 'ds2']);
    });
  });

  describe('Full integration', () => {
    it('generates complete datasource file', () => {
      const ds = defineDatasource('page_views', {
        description: 'Page view tracking data',
        schema: {
          timestamp: t.dateTime(),
          pathname: t.string(),
          session_id: t.string(),
          country: t.string().lowCardinality().nullable(),
        },
        engine: engine.mergeTree({
          sortingKey: ['pathname', 'timestamp'],
          partitionKey: 'toYYYYMM(timestamp)',
          ttl: 'timestamp + INTERVAL 90 DAY',
        }),
      });

      const result = generateDatasource(ds);

      expect(result.name).toBe('page_views');
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Page view tracking data');
      expect(result.content).toContain('SCHEMA >');
      expect(result.content).toContain('timestamp DateTime');
      expect(result.content).toContain('pathname String');
      expect(result.content).toContain('session_id String');
      expect(result.content).toContain('country LowCardinality(Nullable(String))');
      expect(result.content).toContain('ENGINE "MergeTree"');
      expect(result.content).toContain('ENGINE_PARTITION_KEY "toYYYYMM(timestamp)"');
      expect(result.content).toContain('ENGINE_SORTING_KEY "pathname, timestamp"');
      expect(result.content).toContain('ENGINE_TTL "timestamp + INTERVAL 90 DAY"');
    });
  });

  describe('Kafka configuration', () => {
    it('includes Kafka connection name and topic', () => {
      const kafkaConn = createKafkaConnection('my_kafka', {
        bootstrapServers: 'kafka.example.com:9092',
      });

      const ds = defineDatasource('kafka_events', {
        schema: {
          timestamp: t.dateTime(),
          event: t.string(),
        },
        engine: engine.mergeTree({ sortingKey: ['timestamp'] }),
        kafka: {
          connection: kafkaConn,
          topic: 'events',
        },
      });

      const result = generateDatasource(ds);

      expect(result.content).toContain('KAFKA_CONNECTION_NAME my_kafka');
      expect(result.content).toContain('KAFKA_TOPIC events');
    });

    it('includes Kafka group ID when provided', () => {
      const kafkaConn = createKafkaConnection('my_kafka', {
        bootstrapServers: 'kafka.example.com:9092',
      });

      const ds = defineDatasource('kafka_events', {
        schema: {
          timestamp: t.dateTime(),
          event: t.string(),
        },
        engine: engine.mergeTree({ sortingKey: ['timestamp'] }),
        kafka: {
          connection: kafkaConn,
          topic: 'events',
          groupId: 'my-consumer-group',
        },
      });

      const result = generateDatasource(ds);

      expect(result.content).toContain('KAFKA_GROUP_ID my-consumer-group');
    });

    it('includes auto offset reset when provided', () => {
      const kafkaConn = createKafkaConnection('my_kafka', {
        bootstrapServers: 'kafka.example.com:9092',
      });

      const ds = defineDatasource('kafka_events', {
        schema: {
          timestamp: t.dateTime(),
          event: t.string(),
        },
        engine: engine.mergeTree({ sortingKey: ['timestamp'] }),
        kafka: {
          connection: kafkaConn,
          topic: 'events',
          autoOffsetReset: 'earliest',
        },
      });

      const result = generateDatasource(ds);

      expect(result.content).toContain('KAFKA_AUTO_OFFSET_RESET earliest');
    });

    it('generates complete Kafka datasource with all options', () => {
      const kafkaConn = createKafkaConnection('my_kafka', {
        bootstrapServers: 'kafka.example.com:9092',
        securityProtocol: 'SASL_SSL',
        saslMechanism: 'PLAIN',
      });

      const ds = defineDatasource('kafka_events', {
        description: 'Events from Kafka',
        schema: {
          timestamp: t.dateTime(),
          event_type: t.string(),
          payload: t.string(),
        },
        engine: engine.mergeTree({ sortingKey: ['timestamp'] }),
        kafka: {
          connection: kafkaConn,
          topic: 'events',
          groupId: 'my-consumer-group',
          autoOffsetReset: 'earliest',
        },
      });

      const result = generateDatasource(ds);

      expect(result.name).toBe('kafka_events');
      expect(result.content).toContain('DESCRIPTION >');
      expect(result.content).toContain('Events from Kafka');
      expect(result.content).toContain('SCHEMA >');
      expect(result.content).toContain('ENGINE "MergeTree"');
      expect(result.content).toContain('KAFKA_CONNECTION_NAME my_kafka');
      expect(result.content).toContain('KAFKA_TOPIC events');
      expect(result.content).toContain('KAFKA_GROUP_ID my-consumer-group');
      expect(result.content).toContain('KAFKA_AUTO_OFFSET_RESET earliest');
    });
  });
});
