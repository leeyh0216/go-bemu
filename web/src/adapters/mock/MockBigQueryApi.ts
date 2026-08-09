import type {
  Dataset,
  Job,
  Project,
  QueryResult,
  TableDetails,
  TablePreview,
  TableSummary
} from "../../domain/models";
import type { BigQueryApi } from "../../ports/BigQueryApi";

const wait = (milliseconds = 80) => new Promise((resolve) => setTimeout(resolve, milliseconds));

const initialTables: TableDetails[] = [
  {
    projectId: "local-project",
    datasetId: "analytics",
    id: "events",
    type: "TABLE",
    numRows: "12840",
    numBytes: "1842304",
    createdAt: Date.now() - 86400000 * 12,
    modifiedAt: Date.now() - 78000,
    partitioning: "DAY",
    clustering: ["event_name"],
    schema: [
      { name: "event_id", type: "STRING", mode: "REQUIRED" },
      { name: "event_name", type: "STRING", mode: "NULLABLE" },
      { name: "occurred_at", type: "TIMESTAMP", mode: "REQUIRED" },
      { name: "amount", type: "NUMERIC", mode: "NULLABLE" },
      {
        name: "context",
        type: "RECORD",
        mode: "NULLABLE",
        fields: [
          { name: "source", type: "STRING", mode: "NULLABLE" },
          { name: "tags", type: "STRING", mode: "REPEATED" }
        ]
      }
    ]
  },
  {
    projectId: "local-project",
    datasetId: "analytics",
    id: "daily_summary",
    type: "VIEW",
    numRows: "30",
    schema: [
      { name: "event_date", type: "DATE", mode: "NULLABLE" },
      { name: "event_count", type: "INTEGER", mode: "NULLABLE" }
    ]
  },
  {
    projectId: "local-project",
    datasetId: "staging",
    id: "incoming_orders",
    type: "TABLE",
    numRows: "213",
    schema: [
      { name: "order_id", type: "INTEGER", mode: "REQUIRED" },
      { name: "customer", type: "STRING", mode: "NULLABLE" },
      { name: "total", type: "NUMERIC", mode: "NULLABLE" }
    ]
  }
];

export class MockBigQueryApi implements BigQueryApi {
  private projects: Project[] = [
    { id: "local-project", name: "Local project", numericId: "100001" },
    { id: "compatibility-lab", name: "Compatibility lab", numericId: "100002" }
  ];
  private datasets: Dataset[] = [
    { projectId: "local-project", id: "analytics", location: "US" },
    { projectId: "local-project", id: "staging", location: "US" },
    { projectId: "compatibility-lab", id: "matrix", location: "US" }
  ];
  private tables = structuredClone(initialTables);
  private jobs: Job[] = [
    {
      id: "query_01J0K4X2",
      projectId: "local-project",
      type: "QUERY",
      state: "DONE",
      createdAt: Date.now() - 45000,
      startedAt: Date.now() - 44500,
      endedAt: Date.now() - 43900,
      userEmail: "local-user",
      query: "SELECT event_name, COUNT(*) AS total FROM `analytics.events` GROUP BY event_name",
      bytesProcessed: "1842304"
    },
    {
      id: "load_01J0K4RN",
      projectId: "local-project",
      type: "LOAD",
      state: "DONE",
      createdAt: Date.now() - 360000,
      startedAt: Date.now() - 359400,
      endedAt: Date.now() - 356000,
      userEmail: "spark-connector"
    },
    {
      id: "query_01J0K4F8",
      projectId: "local-project",
      type: "QUERY",
      state: "DONE",
      createdAt: Date.now() - 720000,
      startedAt: Date.now() - 719700,
      endedAt: Date.now() - 719100,
      userEmail: "local-user",
      query: "SELECT missing_column FROM `analytics.events`",
      error: "Unrecognized name: missing_column"
    }
  ];

  async listProjects(): Promise<Project[]> {
    await wait();
    return structuredClone(this.projects);
  }

  async createProject(projectId: string, name = projectId): Promise<Project> {
    await wait();
    if (this.projects.some((project) => project.id === projectId)) throw new Error(`Project ${projectId} already exists`);
    const project = { id: projectId, name };
    this.projects.push(project);
    return structuredClone(project);
  }

  async deleteProject(projectId: string): Promise<void> {
    await wait();
    this.projects = this.projects.filter((project) => project.id !== projectId);
    this.datasets = this.datasets.filter((dataset) => dataset.projectId !== projectId);
    this.tables = this.tables.filter((table) => table.projectId !== projectId);
  }

  async listDatasets(projectId: string): Promise<Dataset[]> {
    await wait();
    return structuredClone(this.datasets.filter((dataset) => dataset.projectId === projectId));
  }

  async createDataset(projectId: string, datasetId: string, location = "US"): Promise<Dataset> {
    await wait();
    const dataset = { projectId, id: datasetId, location };
    this.datasets.push(dataset);
    return structuredClone(dataset);
  }

  async deleteDataset(projectId: string, datasetId: string): Promise<void> {
    await wait();
    this.datasets = this.datasets.filter((dataset) => dataset.projectId !== projectId || dataset.id !== datasetId);
    this.tables = this.tables.filter((table) => table.projectId !== projectId || table.datasetId !== datasetId);
  }

  async listTables(projectId: string, datasetId: string): Promise<TableSummary[]> {
    await wait();
    return structuredClone(
      this.tables
        .filter((table) => table.projectId === projectId && table.datasetId === datasetId)
        .map(({ schema: _schema, ...table }) => table)
    );
  }

  async getTable(projectId: string, datasetId: string, tableId: string): Promise<TableDetails> {
    await wait();
    const table = this.tables.find(
      (candidate) => candidate.projectId === projectId && candidate.datasetId === datasetId && candidate.id === tableId
    );
    if (!table) throw new Error(`Table ${datasetId}.${tableId} was not found`);
    return structuredClone(table);
  }

  async previewTable(projectId: string, datasetId: string, tableId: string, limit = 100): Promise<TablePreview> {
    const table = await this.getTable(projectId, datasetId, tableId);
    const rows =
      tableId === "events"
        ? [
            ["evt_10291", "purchase", "2026-08-08T05:19:11Z", "72.40", { source: "web", tags: ["paid"] }],
            ["evt_10290", "page_view", "2026-08-08T05:18:42Z", null, { source: "mobile", tags: [] }],
            ["evt_10289", "sign_up", "2026-08-08T05:17:03Z", null, { source: "web", tags: ["organic"] }]
          ]
        : tableId === "incoming_orders"
          ? [[92813, "Northwind", "128.00"], [92812, "Contoso", "91.50"]]
          : [["2026-08-08", 351], ["2026-08-07", 418]];
    return { schema: table.schema, rows: rows.slice(0, limit), totalRows: table.numRows || String(rows.length) };
  }

  async deleteTable(projectId: string, datasetId: string, tableId: string): Promise<void> {
    await wait();
    this.tables = this.tables.filter(
      (table) => table.projectId !== projectId || table.datasetId !== datasetId || table.id !== tableId
    );
  }

  async runQuery(projectId: string, query: string): Promise<QueryResult> {
    await wait(260);
    const jobId = `query_${Date.now().toString(36)}`;
    const isCount = /count\s*\(/i.test(query);
    const result: QueryResult = isCount
      ? {
          jobId,
          complete: true,
          schema: [
            { name: "event_name", type: "STRING", mode: "NULLABLE" },
            { name: "total", type: "INTEGER", mode: "NULLABLE" }
          ],
          rows: [["page_view", 8421], ["purchase", 904], ["sign_up", 317]],
          totalRows: "3",
          elapsedMs: 238,
          bytesProcessed: "1842304"
        }
      : {
          jobId,
          complete: true,
          schema: initialTables[0].schema,
          rows: (await this.previewTable(projectId, "analytics", "events", 100)).rows,
          totalRows: "3",
          elapsedMs: 238,
          bytesProcessed: "1842304"
        };
    this.jobs.unshift({
      id: jobId,
      projectId,
      type: "QUERY",
      state: "DONE",
      createdAt: Date.now() - 238,
      startedAt: Date.now() - 230,
      endedAt: Date.now(),
      userEmail: "local-user",
      query,
      bytesProcessed: result.bytesProcessed
    });
    return result;
  }

  async listJobs(projectId: string): Promise<Job[]> {
    await wait();
    return structuredClone(this.jobs.filter((job) => job.projectId === projectId));
  }

  async getJob(projectId: string, jobId: string): Promise<Job> {
    await wait();
    const job = this.jobs.find((candidate) => candidate.projectId === projectId && candidate.id === jobId);
    if (!job) throw new Error(`Job ${jobId} was not found`);
    return structuredClone(job);
  }

  async cancelJob(projectId: string, jobId: string): Promise<Job> {
    const job = await this.getJob(projectId, jobId);
    job.state = "DONE";
    job.error = "Job was cancelled";
    return job;
  }
}
