export type Project = {
  id: string;
  name: string;
  numericId?: string;
};

export type Dataset = {
  projectId: string;
  id: string;
  location?: string;
  friendlyName?: string;
};

export type TableSummary = {
  projectId: string;
  datasetId: string;
  id: string;
  type: "TABLE" | "VIEW" | "MATERIALIZED_VIEW" | "EXTERNAL" | string;
};

export type FieldMode = "NULLABLE" | "REQUIRED" | "REPEATED";

export type SchemaField = {
  name: string;
  type: string;
  mode: FieldMode;
  description?: string;
  fields?: SchemaField[];
};

export type TableDetails = TableSummary & {
  schema: SchemaField[];
  numRows?: string;
  numBytes?: string;
  createdAt?: number;
  modifiedAt?: number;
  partitioning?: string;
  clustering?: string[];
  labels?: Record<string, string>;
};

export type QueryCell = string | number | boolean | null | QueryCell[] | Record<string, unknown>;

export type QueryResult = {
  jobId: string;
  complete: boolean;
  schema: SchemaField[];
  rows: QueryCell[][];
  totalRows: string;
  elapsedMs?: number;
  bytesProcessed?: string;
};

export type JobState = "PENDING" | "RUNNING" | "DONE";

export type Job = {
  id: string;
  projectId: string;
  type: "QUERY" | "LOAD" | "EXTRACT" | "COPY" | string;
  state: JobState;
  createdAt?: number;
  startedAt?: number;
  endedAt?: number;
  userEmail?: string;
  query?: string;
  error?: string;
  bytesProcessed?: string;
};

export type TablePreview = {
  schema: SchemaField[];
  rows: QueryCell[][];
  totalRows: string;
};
