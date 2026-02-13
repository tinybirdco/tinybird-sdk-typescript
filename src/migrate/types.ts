export type ResourceKind = "datasource" | "pipe" | "connection";

export interface ResourceFile {
  kind: ResourceKind;
  filePath: string;
  absolutePath: string;
  name: string;
  content: string;
}

export interface MigrationError {
  filePath: string;
  resourceName: string;
  resourceKind: ResourceKind;
  message: string;
}

export interface DatasourceColumnModel {
  name: string;
  type: string;
  jsonPath?: string;
  defaultExpression?: string;
  codec?: string;
}

export interface DatasourceEngineModel {
  type: string;
  sortingKey: string[];
  partitionKey?: string;
  primaryKey?: string[];
  ttl?: string;
  ver?: string;
  sign?: string;
  version?: string;
  summingColumns?: string[];
  settings?: Record<string, string | number | boolean>;
}

export interface DatasourceKafkaModel {
  connectionName: string;
  topic: string;
  groupId?: string;
  autoOffsetReset?: "earliest" | "latest";
}

export interface DatasourceTokenModel {
  name: string;
  scope: "READ" | "APPEND";
}

export interface DatasourceModel {
  kind: "datasource";
  name: string;
  filePath: string;
  description?: string;
  columns: DatasourceColumnModel[];
  engine: DatasourceEngineModel;
  kafka?: DatasourceKafkaModel;
  forwardQuery?: string;
  tokens: DatasourceTokenModel[];
  sharedWith: string[];
}

export interface PipeNodeModel {
  name: string;
  description?: string;
  sql: string;
}

export interface PipeTokenModel {
  name: string;
  scope: "READ";
}

export type PipeTypeModel = "pipe" | "endpoint" | "materialized" | "copy";

export interface PipeParamModel {
  name: string;
  type: string;
  required: boolean;
  defaultValue?: string | number;
}

export interface PipeModel {
  kind: "pipe";
  name: string;
  filePath: string;
  description?: string;
  type: PipeTypeModel;
  nodes: PipeNodeModel[];
  cacheTtl?: number;
  materializedDatasource?: string;
  deploymentMethod?: "alter";
  copyTargetDatasource?: string;
  copySchedule?: string;
  copyMode?: "append" | "replace";
  tokens: PipeTokenModel[];
  params: PipeParamModel[];
  inferredOutputColumns: string[];
}

export interface KafkaConnectionModel {
  kind: "connection";
  name: string;
  filePath: string;
  connectionType: "kafka";
  bootstrapServers: string;
  securityProtocol?: "SASL_SSL" | "PLAINTEXT" | "SASL_PLAINTEXT";
  saslMechanism?: "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "OAUTHBEARER";
  key?: string;
  secret?: string;
  sslCaPem?: string;
}

export type ParsedResource = DatasourceModel | PipeModel | KafkaConnectionModel;

export interface MigrationResult {
  success: boolean;
  outputPath: string;
  migrated: ParsedResource[];
  errors: MigrationError[];
  dryRun: boolean;
  outputContent?: string;
}

