import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AppShell } from "./components/AppShell";
import { LoadingView } from "./components/StateViews";
import { useApi } from "./context/ApiContext";
import { ResourceExplorer } from "./features/explorer/ResourceExplorer";

const TableWorkspace = lazy(() =>
  import("./features/explorer/TableWorkspace").then((module) => ({ default: module.TableWorkspace }))
);
const JobsWorkspace = lazy(() =>
  import("./features/jobs/JobsWorkspace").then((module) => ({ default: module.JobsWorkspace }))
);
const QueryWorkspace = lazy(() =>
  import("./features/query/QueryWorkspace").then((module) => ({ default: module.QueryWorkspace }))
);

type Selection = { datasetId: string; tableId: string };
type Route =
  | { view: "query"; table?: string }
  | { view: "jobs" }
  | { view: "table"; datasetId: string; tableId: string };

const parseRoute = (): Route => {
  const raw = window.location.hash.replace(/^#/, "") || "/query";
  const [pathname, query = ""] = raw.split("?", 2);
  const table = pathname.match(/^\/table\/([^/]+)\/([^/]+)$/);
  if (table) {
    return { view: "table", datasetId: decodeURIComponent(table[1]), tableId: decodeURIComponent(table[2]) };
  }
  if (pathname === "/jobs") return { view: "jobs" };
  return { view: "query", table: new URLSearchParams(query).get("table") || undefined };
};

const navigate = (target: string) => {
  window.location.hash = target;
};

export default function App() {
  const api = useApi();
  const projects = useQuery({ queryKey: ["projects"], queryFn: () => api.listProjects() });
  const [projectId, setProjectId] = useState(() => localStorage.getItem("bqemu.project") || "");
  const [route, setRoute] = useState<Route>(parseRoute);

  useEffect(() => {
    const update = () => setRoute(parseRoute());
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);

  useEffect(() => {
    if (!projectId && projects.data?.length) setProjectId(projects.data[0].id);
  }, [projectId, projects.data]);

  useEffect(() => {
    if (projectId) localStorage.setItem("bqemu.project", projectId);
  }, [projectId]);

  const selection = useMemo<Selection | undefined>(
    () => route.view === "table" ? { datasetId: route.datasetId, tableId: route.tableId } : undefined,
    [route]
  );

  const explorer = (
    <ResourceExplorer
      projectId={projectId}
      selection={selection}
      onSelect={({ datasetId, tableId }) => navigate(`/table/${encodeURIComponent(datasetId)}/${encodeURIComponent(tableId)}`)}
    />
  );

  return (
    <AppShell
      projectId={projectId}
      onProjectChange={setProjectId}
      activeView={route.view}
      navigationKey={route.view === "table" ? `${route.datasetId}/${route.tableId}` : route.view}
      onNavigate={(view) => navigate(`/${view}`)}
      explorer={explorer}
    >
      <Suspense fallback={<LoadingView label="Loading workspace" />}>
        {route.view === "query" && <QueryWorkspace projectId={projectId} insertedTable={route.table} />}
        {route.view === "jobs" && <JobsWorkspace projectId={projectId} />}
        {route.view === "table" && projectId && (
          <TableWorkspace
            projectId={projectId}
            datasetId={route.datasetId}
            tableId={route.tableId}
            onQuery={(table) => navigate(`/query?table=${encodeURIComponent(table)}`)}
            onDeleted={() => navigate("/query")}
          />
        )}
      </Suspense>
    </AppShell>
  );
}
