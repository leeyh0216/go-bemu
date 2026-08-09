import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Box,
  Button,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  MenuItem,
  Select,
  Stack,
  TextField,
  Tooltip,
  Typography
} from "@mui/material";
import {
  ChevronDown,
  ChevronRight,
  Database,
  Eye,
  Folder,
  Plus,
  RefreshCw,
  Table2,
  Trash2
} from "lucide-react";
import { EmptyView, ErrorView, LoadingView } from "../../components/StateViews";
import { useApi } from "../../context/ApiContext";
import type { Dataset, TableSummary } from "../../domain/models";

type Selection = { datasetId: string; tableId: string };

function DatasetNode({
  dataset,
  selection,
  onSelect,
  onDelete
}: {
  dataset: Dataset;
  selection?: Selection;
  onSelect: (selection: Selection) => void;
  onDelete: (dataset: Dataset) => void;
}) {
  const api = useApi();
  const [expanded, setExpanded] = useState(true);
  const tables = useQuery({
    queryKey: ["tables", dataset.projectId, dataset.id],
    queryFn: () => api.listTables(dataset.projectId, dataset.id),
    enabled: expanded
  });

  const tableIcon = (table: TableSummary) =>
    table.type.includes("VIEW") ? <Eye size={15} /> : <Table2 size={15} />;

  return (
    <Box>
      <ListItemButton onClick={() => setExpanded((current) => !current)} sx={{ minHeight: 36, pr: 0.5 }}>
        <ListItemIcon sx={{ minWidth: 30 }}>{expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}</ListItemIcon>
        <Folder size={16} style={{ marginRight: 8 }} />
        <ListItemText
          primary={dataset.id}
          secondary={dataset.location}
          slotProps={{ primary: { variant: "body2", noWrap: true }, secondary: { variant: "caption" } }}
        />
        <Tooltip title="Delete dataset">
          <IconButton
            size="small"
            aria-label={`Delete ${dataset.id}`}
            onClick={(event) => {
              event.stopPropagation();
              onDelete(dataset);
            }}
          >
            <Trash2 size={14} />
          </IconButton>
        </Tooltip>
      </ListItemButton>
      <Collapse in={expanded} unmountOnExit>
        {tables.isLoading && <Box sx={{ py: 1 }}><LoadingView label="Loading tables" /></Box>}
        {tables.isError && <ErrorView error={tables.error} onRetry={() => tables.refetch()} />}
        {tables.data?.map((table) => {
          const selected = selection?.datasetId === dataset.id && selection.tableId === table.id;
          return (
            <ListItemButton
              key={table.id}
              selected={selected}
              onClick={() => onSelect({ datasetId: dataset.id, tableId: table.id })}
              sx={{ minHeight: 34, pl: 5.25 }}
            >
              <ListItemIcon sx={{ minWidth: 28 }}>{tableIcon(table)}</ListItemIcon>
              <ListItemText primary={table.id} slotProps={{ primary: { variant: "body2", noWrap: true } }} />
            </ListItemButton>
          );
        })}
        {tables.isSuccess && tables.data.length === 0 && (
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", py: 1, pl: 6 }}>
            No tables
          </Typography>
        )}
      </Collapse>
    </Box>
  );
}

export function ResourceExplorer({
  projectId,
  selection,
  onSelect
}: {
  projectId?: string;
  selection?: Selection;
  onSelect: (selection: Selection) => void;
}) {
  const api = useApi();
  const queryClient = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [datasetId, setDatasetId] = useState("");
  const [location, setLocation] = useState("US");
  const datasets = useQuery({
    queryKey: ["datasets", projectId],
    queryFn: () => api.listDatasets(projectId!),
    enabled: Boolean(projectId)
  });
  const create = useMutation({
    mutationFn: () => api.createDataset(projectId!, datasetId.trim(), location),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["datasets", projectId] });
      setDatasetId("");
      setCreateOpen(false);
    }
  });
  const remove = useMutation({
    mutationFn: (dataset: Dataset) => api.deleteDataset(dataset.projectId, dataset.id, true),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["datasets", projectId] })
  });

  return (
    <Box sx={{ height: "100%", display: "flex", flexDirection: "column", bgcolor: "background.paper" }}>
      <Stack direction="row" sx={{ minHeight: 48, px: 1.5, alignItems: "center", borderBottom: 1, borderColor: "divider" }}>
        <Database size={17} />
        <Typography variant="h3" sx={{ ml: 1, flex: 1 }}>Explorer</Typography>
        <Tooltip title="Create dataset">
          <span>
            <IconButton size="small" disabled={!projectId} onClick={() => setCreateOpen(true)} aria-label="Create dataset">
              <Plus size={16} />
            </IconButton>
          </span>
        </Tooltip>
        <Tooltip title="Refresh">
          <IconButton size="small" onClick={() => datasets.refetch()} aria-label="Refresh explorer">
            <RefreshCw size={15} />
          </IconButton>
        </Tooltip>
      </Stack>
      <Box sx={{ overflowY: "auto", flex: 1 }}>
        {!projectId && <EmptyView title="Select a project" />}
        {datasets.isLoading && <LoadingView label="Loading datasets" />}
        {datasets.isError && <ErrorView error={datasets.error} onRetry={() => datasets.refetch()} />}
        {datasets.isSuccess && datasets.data.length === 0 && (
          <EmptyView
            title="No datasets"
            action={<Button size="small" startIcon={<Plus size={15} />} onClick={() => setCreateOpen(true)}>Create dataset</Button>}
          />
        )}
        <List dense disablePadding>
          {datasets.data?.map((dataset) => (
            <DatasetNode
              key={dataset.id}
              dataset={dataset}
              selection={selection}
              onSelect={onSelect}
              onDelete={(candidate) => remove.mutate(candidate)}
            />
          ))}
        </List>
        {(create.error || remove.error) && <ErrorView error={create.error || remove.error} />}
      </Box>

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Create dataset</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2} sx={{ pt: 0.5 }}>
            <TextField
              autoFocus
              label="Dataset ID"
              size="small"
              value={datasetId}
              onChange={(event) => setDatasetId(event.target.value)}
              slotProps={{ htmlInput: { pattern: "[A-Za-z_][A-Za-z0-9_]*" } }}
            />
            <Select size="small" value={location} onChange={(event) => setLocation(event.target.value)} aria-label="Location">
              <MenuItem value="US">US</MenuItem>
              <MenuItem value="EU">EU</MenuItem>
              <MenuItem value="asia-northeast3">Seoul</MenuItem>
            </Select>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button variant="contained" disabled={!datasetId.trim() || create.isPending} onClick={() => create.mutate()}>
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
