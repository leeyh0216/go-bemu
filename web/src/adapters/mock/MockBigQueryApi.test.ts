import { describe, expect, it } from "vitest";
import { MockBigQueryApi } from "./MockBigQueryApi";

describe("MockBigQueryApi", () => {
  it("keeps project, dataset and table boundaries consistent", async () => {
    const api = new MockBigQueryApi();
    const project = await api.createProject("new-project", "New project");
    await api.createDataset(project.id, "warehouse", "EU");

    expect((await api.listProjects()).map((item) => item.id)).toContain("new-project");
    expect(await api.listDatasets("new-project")).toEqual([
      { projectId: "new-project", id: "warehouse", location: "EU" }
    ]);

    await api.deleteProject("new-project");
    expect(await api.listDatasets("new-project")).toEqual([]);
  });

  it("records query jobs and returns typed results", async () => {
    const api = new MockBigQueryApi();
    const before = await api.listJobs("local-project");
    const result = await api.runQuery("local-project", "SELECT event_name, COUNT(*) FROM `analytics.events` GROUP BY 1");
    const after = await api.listJobs("local-project");

    expect(result.schema.map((field) => field.name)).toEqual(["event_name", "total"]);
    expect(result.rows).toHaveLength(3);
    expect(after).toHaveLength(before.length + 1);
    expect(after[0].id).toBe(result.jobId);
  });
});
