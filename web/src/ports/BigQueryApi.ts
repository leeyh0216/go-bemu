import type {
  Dataset,
  Job,
  Project,
  QueryResult,
  TableDetails,
  TablePreview,
  TableSummary
} from "../domain/models";

export interface BigQueryApi {
  listProjects(): Promise<Project[]>;
  createProject(projectId: string, name?: string): Promise<Project>;
  deleteProject(projectId: string): Promise<void>;

  listDatasets(projectId: string): Promise<Dataset[]>;
  createDataset(projectId: string, datasetId: string, location?: string): Promise<Dataset>;
  deleteDataset(projectId: string, datasetId: string, deleteContents?: boolean): Promise<void>;

  listTables(projectId: string, datasetId: string): Promise<TableSummary[]>;
  getTable(projectId: string, datasetId: string, tableId: string): Promise<TableDetails>;
  previewTable(projectId: string, datasetId: string, tableId: string, limit?: number): Promise<TablePreview>;
  deleteTable(projectId: string, datasetId: string, tableId: string): Promise<void>;

  runQuery(projectId: string, query: string): Promise<QueryResult>;
  listJobs(projectId: string): Promise<Job[]>;
  getJob(projectId: string, jobId: string, location?: string): Promise<Job>;
  cancelJob(projectId: string, jobId: string, location?: string): Promise<Job>;
}
