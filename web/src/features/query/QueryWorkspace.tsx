import { useMemo, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import {
  Box,
  Button,
  Chip,
  Divider,
  IconButton,
  Stack,
  Tab,
  Tabs,
  Tooltip,
  Typography
} from "@mui/material";
import { Eraser, Play, Rows3, Timer, Waypoints } from "lucide-react";
import { useApi } from "../../context/ApiContext";
import { ErrorView } from "../../components/StateViews";
import { ResultTable } from "../../components/ResultTable";
import { formatBytes, formatCount } from "../../components/format";

const initialSql = `SELECT
  event_name,
  COUNT(*) AS total
FROM \`analytics.events\`
GROUP BY event_name
ORDER BY total DESC`;

export function QueryWorkspace({ projectId, insertedTable }: { projectId?: string; insertedTable?: string }) {
  const api = useApi();
  const queryClient = useQueryClient();
  const [query, setQuery] = useState(initialSql);
  const [tab, setTab] = useState(0);
  const run = useMutation({
    mutationFn: () => api.runQuery(projectId!, query),
    onSuccess: () => {
      setTab(0);
      queryClient.invalidateQueries({ queryKey: ["jobs", projectId] });
    }
  });

  const effectiveSql = useMemo(() => {
    if (!insertedTable) return query;
    return query === initialSql ? `SELECT * FROM \`${insertedTable}\` LIMIT 100` : query;
  }, [insertedTable, query]);

  const execute = () => {
    if (projectId && effectiveSql.trim() && !run.isPending) {
      if (effectiveSql !== query) setQuery(effectiveSql);
      run.mutate();
    }
  };

  return (
    <Box sx={{ height: "100%", minHeight: 560, display: "grid", gridTemplateRows: "auto minmax(220px, 42%) auto minmax(240px, 1fr)" }}>
      <Stack direction="row" spacing={1} sx={{ minHeight: 50, px: 2, alignItems: "center", bgcolor: "background.paper", borderBottom: 1, borderColor: "divider" }}>
        <Button
          variant="contained"
          startIcon={<Play size={16} fill="currentColor" />}
          disabled={!projectId || !effectiveSql.trim() || run.isPending}
          onClick={execute}
        >
          {run.isPending ? "Running" : "Run"}
        </Button>
        <Tooltip title="Clear editor">
          <IconButton size="small" aria-label="Clear editor" onClick={() => setQuery("")}><Eraser size={16} /></IconButton>
        </Tooltip>
        <Divider orientation="vertical" flexItem sx={{ my: 1 }} />
        <Typography variant="body2" color="text.secondary" noWrap>{projectId || "No project selected"}</Typography>
      </Stack>

      <Box
        sx={{ bgcolor: "#ffffff", overflow: "hidden" }}
        onKeyDownCapture={(event) => {
          if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
            event.preventDefault();
            execute();
          }
        }}
      >
        <CodeMirror
          value={effectiveSql}
          height="100%"
          minHeight="220px"
          extensions={[sql()]}
          onChange={setQuery}
          basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true, bracketMatching: true }}
          theme="light"
          aria-label="SQL editor"
          style={{ height: "100%", fontSize: 13 }}
        />
      </Box>

      <Stack direction="row" sx={{ minHeight: 45, px: 1.5, alignItems: "center", bgcolor: "background.paper", borderTop: 1, borderBottom: 1, borderColor: "divider" }}>
        <Tabs value={tab} onChange={(_, value) => setTab(value)} sx={{ minHeight: 44 }}>
          <Tab label="Results" sx={{ minHeight: 44 }} />
          <Tab label="Execution" sx={{ minHeight: 44 }} />
        </Tabs>
        <Box sx={{ flex: 1 }} />
        {run.data && (
          <Stack direction="row" spacing={0.75} sx={{ display: { xs: "none", sm: "flex" } }}>
            <Chip size="small" variant="outlined" icon={<Rows3 size={14} />} label={`${formatCount(run.data.totalRows)} rows`} />
            <Chip size="small" variant="outlined" icon={<Timer size={14} />} label={`${run.data.elapsedMs || 0} ms`} />
            <Chip size="small" variant="outlined" icon={<Waypoints size={14} />} label={formatBytes(run.data.bytesProcessed)} />
          </Stack>
        )}
      </Stack>

      <Box sx={{ minHeight: 0, overflow: "auto", bgcolor: "background.paper" }}>
        {run.error && <ErrorView error={run.error} />}
        {!run.data && !run.error && (
          <Box sx={{ height: "100%", display: "grid", placeItems: "center" }}>
            <Typography variant="body2" color="text.secondary">Results</Typography>
          </Box>
        )}
        {run.data && tab === 0 && <ResultTable schema={run.data.schema} rows={run.data.rows} />}
        {run.data && tab === 1 && (
          <Box sx={{ p: 2.5, display: "grid", gridTemplateColumns: "max-content minmax(0, 1fr)", gap: "10px 24px" }}>
            <Typography variant="body2" color="text.secondary">Job ID</Typography>
            <Typography variant="body2" sx={{ fontFamily: "monospace", overflowWrap: "anywhere" }}>{run.data.jobId}</Typography>
            <Typography variant="body2" color="text.secondary">State</Typography>
            <Typography variant="body2">{run.data.complete ? "DONE" : "RUNNING"}</Typography>
            <Typography variant="body2" color="text.secondary">Rows</Typography>
            <Typography variant="body2">{formatCount(run.data.totalRows)}</Typography>
            <Typography variant="body2" color="text.secondary">Processed</Typography>
            <Typography variant="body2">{formatBytes(run.data.bytesProcessed)}</Typography>
          </Box>
        )}
      </Box>
    </Box>
  );
}
