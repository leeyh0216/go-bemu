import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  Stack,
  Tab,
  Tabs,
  Typography
} from "@mui/material";
import { Eye, Play, Table2, Trash2 } from "lucide-react";
import { ErrorView, LoadingView } from "../../components/StateViews";
import { ResultTable } from "../../components/ResultTable";
import { formatBytes, formatCount, formatTime } from "../../components/format";
import { useApi } from "../../context/ApiContext";
import type { SchemaField } from "../../domain/models";

function SchemaRows({ fields, depth = 0 }: { fields: SchemaField[]; depth?: number }) {
  return fields.map((field) => (
    <Box key={`${depth}-${field.name}`}>
      <Box sx={{ minHeight: 43, px: 2, display: "grid", gridTemplateColumns: "minmax(180px, 1.5fr) minmax(100px, .8fr) minmax(90px, .7fr) minmax(180px, 2fr)", alignItems: "center", borderBottom: 1, borderColor: "divider" }}>
        <Typography variant="body2" sx={{ pl: depth * 2 }}>{field.name}</Typography>
        <Typography variant="body2" sx={{ fontFamily: "monospace" }}>{field.type}</Typography>
        <Typography variant="body2" color="text.secondary">{field.mode}</Typography>
        <Typography variant="body2" color="text.secondary">{field.description || "-"}</Typography>
      </Box>
      {field.fields && <SchemaRows fields={field.fields} depth={depth + 1} />}
    </Box>
  ));
}

export function TableWorkspace({
  projectId,
  datasetId,
  tableId,
  onQuery,
  onDeleted
}: {
  projectId: string;
  datasetId: string;
  tableId: string;
  onQuery: (fullTableName: string) => void;
  onDeleted: () => void;
}) {
  const api = useApi();
  const queryClient = useQueryClient();
  const [tab, setTab] = useState(0);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const table = useQuery({
    queryKey: ["table", projectId, datasetId, tableId],
    queryFn: () => api.getTable(projectId, datasetId, tableId)
  });
  const preview = useQuery({
    queryKey: ["preview", projectId, datasetId, tableId],
    queryFn: () => api.previewTable(projectId, datasetId, tableId, 100),
    enabled: tab === 1
  });
  const remove = useMutation({
    mutationFn: () => api.deleteTable(projectId, datasetId, tableId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tables", projectId, datasetId] });
      onDeleted();
    }
  });

  if (table.isLoading) return <LoadingView label="Loading table" />;
  if (table.isError) return <ErrorView error={table.error} onRetry={() => table.refetch()} />;
  const details = table.data!;
  const fullName = `${projectId}.${datasetId}.${tableId}`;

  return (
    <Box sx={{ minHeight: "100%", bgcolor: "background.paper" }}>
      <Box sx={{ px: { xs: 2, md: 3 }, py: 2, borderBottom: 1, borderColor: "divider" }}>
        <Stack direction={{ xs: "column", sm: "row" }} spacing={1.5} sx={{ alignItems: { sm: "center" } }}>
          {details.type.includes("VIEW") ? <Eye size={22} /> : <Table2 size={22} />}
          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography variant="h1" noWrap>{tableId}</Typography>
            <Typography variant="body2" color="text.secondary" noWrap>{fullName}</Typography>
          </Box>
          <Button startIcon={<Play size={16} />} onClick={() => onQuery(fullName)}>Query</Button>
          <Button color="error" startIcon={<Trash2 size={16} />} onClick={() => setConfirmDelete(true)}>Delete</Button>
        </Stack>
        <Stack direction="row" spacing={0.75} sx={{ mt: 1.5, flexWrap: "wrap", gap: 0.5 }}>
          <Chip size="small" label={details.type} variant="outlined" />
          <Chip size="small" label={`${formatCount(details.numRows)} rows`} variant="outlined" />
          <Chip size="small" label={formatBytes(details.numBytes)} variant="outlined" />
          {details.partitioning && <Chip size="small" label={`${details.partitioning} partitioned`} variant="outlined" />}
        </Stack>
      </Box>

      <Tabs value={tab} onChange={(_, value) => setTab(value)} sx={{ px: 2, borderBottom: 1, borderColor: "divider" }}>
        <Tab label="Schema" />
        <Tab label="Preview" />
        <Tab label="Details" />
      </Tabs>

      {tab === 0 && (
        <Box sx={{ overflowX: "auto" }}>
          <Box sx={{ minWidth: 720 }}>
            <Box sx={{ minHeight: 39, px: 2, display: "grid", gridTemplateColumns: "minmax(180px, 1.5fr) minmax(100px, .8fr) minmax(90px, .7fr) minmax(180px, 2fr)", alignItems: "center", bgcolor: "#f5f7fa", borderBottom: 1, borderColor: "divider" }}>
              {['Field', 'Type', 'Mode', 'Description'].map((label) => <Typography key={label} variant="body2" sx={{ fontWeight: 600 }}>{label}</Typography>)}
            </Box>
            <SchemaRows fields={details.schema} />
          </Box>
        </Box>
      )}
      {tab === 1 && preview.isLoading && <LoadingView label="Loading preview" />}
      {tab === 1 && preview.isError && <ErrorView error={preview.error} onRetry={() => preview.refetch()} />}
      {tab === 1 && preview.data && <ResultTable schema={preview.data.schema} rows={preview.data.rows} />}
      {tab === 2 && (
        <Box sx={{ p: 3, display: "grid", gridTemplateColumns: { xs: "1fr", sm: "180px minmax(0, 1fr)" }, gap: "12px 28px", maxWidth: 850 }}>
          <Typography variant="body2" color="text.secondary">Table ID</Typography><Typography variant="body2">{fullName}</Typography>
          <Typography variant="body2" color="text.secondary">Created</Typography><Typography variant="body2">{formatTime(details.createdAt)}</Typography>
          <Typography variant="body2" color="text.secondary">Last modified</Typography><Typography variant="body2">{formatTime(details.modifiedAt)}</Typography>
          <Typography variant="body2" color="text.secondary">Rows</Typography><Typography variant="body2">{formatCount(details.numRows)}</Typography>
          <Typography variant="body2" color="text.secondary">Storage</Typography><Typography variant="body2">{formatBytes(details.numBytes)}</Typography>
          <Typography variant="body2" color="text.secondary">Clustering</Typography><Typography variant="body2">{details.clustering?.join(", ") || "-"}</Typography>
          <Typography variant="body2" color="text.secondary">Labels</Typography><Typography variant="body2">{Object.entries(details.labels || {}).map(([key, value]) => `${key}=${value}`).join(", ") || "-"}</Typography>
        </Box>
      )}

      <Dialog open={confirmDelete} onClose={() => setConfirmDelete(false)}>
        <DialogTitle>Delete table</DialogTitle>
        <DialogContent><Typography variant="body2">Delete {datasetId}.{tableId}?</Typography>{remove.error && <ErrorView error={remove.error} />}</DialogContent>
        <Divider />
        <DialogActions>
          <Button onClick={() => setConfirmDelete(false)}>Cancel</Button>
          <Button variant="contained" color="error" disabled={remove.isPending} onClick={() => remove.mutate()}>Delete</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
