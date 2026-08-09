import type {
  Dataset,
  Job,
  JobState,
  Project,
  QueryCell,
  QueryResult,
  SchemaField,
  TableDetails,
  TablePreview,
  TableSummary
} from "../../domain/models";
import type { BigQueryApi } from "../../ports/BigQueryApi";

type Json = Record<string, unknown>;

export class BigQueryApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly reason?: string
  ) {
    super(message);
    this.name = "BigQueryApiError";
  }
}

const ref = (value: unknown): Json => (value && typeof value === "object" ? (value as Json) : {});
const list = (value: unknown): Json[] => (Array.isArray(value) ? value.map(ref) : []);
const text = (value: unknown): string | undefined =>
  typeof value === "string" || typeof value === "number" ? String(value) : undefined;
const number = (value: unknown): number | undefined => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const fieldsFrom = (schema: unknown): SchemaField[] =>
  list(ref(schema).fields).map((field) => ({
    name: text(field.name) || "",
    type: text(field.type) || "STRING",
    mode: (text(field.mode) || "NULLABLE") as SchemaField["mode"],
    description: text(field.description),
    fields: field.fields ? fieldsFrom({ fields: field.fields }) : undefined
  }));

const decodeValue = (value: unknown): QueryCell => {
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) return value.map(decodeValue);
  if (value && typeof value === "object") {
    const object = value as Json;
    if ("v" in object) return decodeValue(object.v);
    return Object.fromEntries(Object.entries(object).map(([key, item]) => [key, decodeValue(item)]));
  }
  return String(value);
};

const rowsFrom = (rows: unknown): QueryCell[][] =>
  list(rows).map((row) => list(row.f).map((cell) => decodeValue(cell.v)));

export class HttpBigQueryApi implements BigQueryApi {
  constructor(private readonly baseUrl = "") {}

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers: { "content-type": "application/json", ...init?.headers }
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const error = ref(ref(body).error);
      const first = list(error.errors)[0] || {};
      throw new BigQueryApiError(
        text(error.message) || `${response.status} ${response.statusText}`,
        response.status,
        text(first.reason)
      );
    }
    if (response.status === 204 || response.headers.get("content-length") === "0") return undefined as T;
    return (await response.json()) as T;
  }

  async listProjects(): Promise<Project[]> {
    const body = await this.request<Json>("/bigquery/v2/projects");
    return list(body.projects).map((item) => {
      const project = ref(item.projectReference);
      const id = text(project.projectId) || text(item.id) || "";
      return { id, name: text(item.friendlyName) || id, numericId: text(item.numericId) };
    });
  }

  async createProject(projectId: string, name = projectId): Promise<Project> {
    const body = await this.request<Json>("/emulator/v1/projects", {
      method: "POST",
      body: JSON.stringify({ projectId, friendlyName: name })
    });
    return { id: text(body.projectId) || projectId, name: text(body.friendlyName) || name };
  }

  async deleteProject(projectId: string): Promise<void> {
    await this.request(`/emulator/v1/projects/${encodeURIComponent(projectId)}`, { method: "DELETE" });
  }

  async listDatasets(projectId: string): Promise<Dataset[]> {
    const body = await this.request<Json>(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets`);
    return list(body.datasets).map((item) => {
      const dataset = ref(item.datasetReference);
      return {
        projectId: text(dataset.projectId) || projectId,
        id: text(dataset.datasetId) || "",
        location: text(item.location),
        friendlyName: text(item.friendlyName)
      };
    });
  }

  async createDataset(projectId: string, datasetId: string, location = "US"): Promise<Dataset> {
    const body = await this.request<Json>(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets`, {
      method: "POST",
      body: JSON.stringify({ datasetReference: { projectId, datasetId }, location })
    });
    const dataset = ref(body.datasetReference);
    return {
      projectId: text(dataset.projectId) || projectId,
      id: text(dataset.datasetId) || datasetId,
      location: text(body.location) || location,
      friendlyName: text(body.friendlyName)
    };
  }

  async deleteDataset(projectId: string, datasetId: string, deleteContents = false): Promise<void> {
    await this.request(
      `/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}?deleteContents=${deleteContents}`,
      { method: "DELETE" }
    );
  }

  async listTables(projectId: string, datasetId: string): Promise<TableSummary[]> {
    const body = await this.request<Json>(
      `/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables`
    );
    return list(body.tables).map((item) => {
      const table = ref(item.tableReference);
      return {
        projectId: text(table.projectId) || projectId,
        datasetId: text(table.datasetId) || datasetId,
        id: text(table.tableId) || "",
        type: text(item.type) || "TABLE"
      };
    });
  }

  async getTable(projectId: string, datasetId: string, tableId: string): Promise<TableDetails> {
    const body = await this.request<Json>(
      `/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}`
    );
    const table = ref(body.tableReference);
    const partition = ref(body.timePartitioning);
    const clustering = ref(body.clustering);
    return {
      projectId: text(table.projectId) || projectId,
      datasetId: text(table.datasetId) || datasetId,
      id: text(table.tableId) || tableId,
      type: text(body.type) || "TABLE",
      schema: fieldsFrom(body.schema),
      numRows: text(body.numRows),
      numBytes: text(body.numBytes),
      createdAt: number(body.creationTime),
      modifiedAt: number(body.lastModifiedTime),
      partitioning: text(partition.type),
      clustering: Array.isArray(clustering.fields) ? clustering.fields.map(String) : undefined,
      labels: ref(body.labels) as Record<string, string>
    };
  }

  async previewTable(projectId: string, datasetId: string, tableId: string, limit = 100): Promise<TablePreview> {
    const [details, body] = await Promise.all([
      this.getTable(projectId, datasetId, tableId),
      this.request<Json>(
        `/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}/data?maxResults=${limit}`
      )
    ]);
    return { schema: details.schema, rows: rowsFrom(body.rows), totalRows: text(body.totalRows) || "0" };
  }

  async deleteTable(projectId: string, datasetId: string, tableId: string): Promise<void> {
    await this.request(
      `/bigquery/v2/projects/${encodeURIComponent(projectId)}/datasets/${encodeURIComponent(datasetId)}/tables/${encodeURIComponent(tableId)}`,
      { method: "DELETE" }
    );
  }

  async runQuery(projectId: string, query: string): Promise<QueryResult> {
    const started = performance.now();
    const body = await this.request<Json>(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/queries`, {
      method: "POST",
      body: JSON.stringify({ query, useLegacySql: false, timeoutMs: 30000 })
    });
    const job = ref(body.jobReference);
    return {
      jobId: text(job.jobId) || "query",
      complete: body.jobComplete !== false,
      schema: fieldsFrom(body.schema),
      rows: rowsFrom(body.rows),
      totalRows: text(body.totalRows) || "0",
      elapsedMs: Math.round(performance.now() - started),
      bytesProcessed: text(body.totalBytesProcessed)
    };
  }

  async listJobs(projectId: string): Promise<Job[]> {
    const body = await this.request<Json>(`/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs?allUsers=true`);
    return list(body.jobs).map((item) => this.mapJob(item, projectId));
  }

  async getJob(projectId: string, jobId: string, location?: string): Promise<Job> {
    const suffix = location ? `?location=${encodeURIComponent(location)}` : "";
    const body = await this.request<Json>(
      `/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}${suffix}`
    );
    return this.mapJob(body, projectId);
  }

  async cancelJob(projectId: string, jobId: string, location?: string): Promise<Job> {
    const suffix = location ? `?location=${encodeURIComponent(location)}` : "";
    const body = await this.request<Json>(
      `/bigquery/v2/projects/${encodeURIComponent(projectId)}/jobs/${encodeURIComponent(jobId)}/cancel${suffix}`,
      { method: "POST", body: "{}" }
    );
    return this.mapJob(ref(body.job), projectId);
  }

  private mapJob(value: unknown, projectId: string): Job {
    const item = ref(value);
    const jobRef = ref(item.jobReference);
    const configuration = ref(item.configuration);
    const status = ref(item.status);
    const statistics = ref(item.statistics);
    const query = ref(configuration.query);
    const error = ref(status.errorResult);
    const type = configuration.query ? "QUERY" : configuration.load ? "LOAD" : configuration.extract ? "EXTRACT" : "COPY";
    return {
      id: text(jobRef.jobId) || text(item.id) || "",
      projectId: text(jobRef.projectId) || projectId,
      type,
      state: (text(status.state) || "PENDING") as JobState,
      createdAt: number(statistics.creationTime),
      startedAt: number(statistics.startTime),
      endedAt: number(statistics.endTime),
      userEmail: text(item.userEmail),
      query: text(query.query),
      error: text(error.message),
      bytesProcessed: text(ref(statistics.query).totalBytesProcessed)
    };
  }
}
