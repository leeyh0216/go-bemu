import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Box,
  Button,
  Divider,
  FormControl,
  IconButton,
  InputAdornment,
  MenuItem,
  Select,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme
} from "@mui/material";
import { RefreshCw, Search, Square, X } from "lucide-react";
import { EmptyView, ErrorView, LoadingView } from "../../components/StateViews";
import { StatusChip } from "../../components/StatusChip";
import { formatBytes, formatDuration, formatTime } from "../../components/format";
import { useApi } from "../../context/ApiContext";
import type { Job } from "../../domain/models";

function JobDetails({ job, onClose, onCancel }: { job: Job; onClose: () => void; onCancel: () => void }) {
  return (
    <Box sx={{ width: { xs: "100%", lg: 390 }, minWidth: 0, height: "100%", borderLeft: { lg: 1 }, borderTop: { xs: 1, lg: 0 }, borderColor: "divider", bgcolor: "background.paper", overflow: "auto" }}>
      <Stack direction="row" sx={{ minHeight: 52, px: 2, alignItems: "center", borderBottom: 1, borderColor: "divider" }}>
        <Typography variant="h3" sx={{ flex: 1 }}>Job details</Typography>
        <Tooltip title="Close"><IconButton size="small" onClick={onClose} aria-label="Close job details"><X size={17} /></IconButton></Tooltip>
      </Stack>
      <Box sx={{ p: 2, display: "grid", gridTemplateColumns: "105px minmax(0, 1fr)", gap: "11px 16px" }}>
        <Typography variant="body2" color="text.secondary">Status</Typography><Box><StatusChip job={job} /></Box>
        <Typography variant="body2" color="text.secondary">Job ID</Typography><Typography variant="body2" sx={{ fontFamily: "monospace", overflowWrap: "anywhere" }}>{job.id}</Typography>
        <Typography variant="body2" color="text.secondary">Type</Typography><Typography variant="body2">{job.type}</Typography>
        <Typography variant="body2" color="text.secondary">Created</Typography><Typography variant="body2">{formatTime(job.createdAt)}</Typography>
        <Typography variant="body2" color="text.secondary">Duration</Typography><Typography variant="body2">{formatDuration(job.startedAt, job.endedAt)}</Typography>
        <Typography variant="body2" color="text.secondary">Processed</Typography><Typography variant="body2">{formatBytes(job.bytesProcessed)}</Typography>
        <Typography variant="body2" color="text.secondary">Principal</Typography><Typography variant="body2">{job.userEmail || "-"}</Typography>
      </Box>
      {job.query && (
        <>
          <Divider />
          <Box sx={{ p: 2 }}>
            <Typography variant="body2" sx={{ mb: 1, fontWeight: 600 }}>SQL</Typography>
            <Box component="pre" sx={{ m: 0, p: 1.5, bgcolor: "#f5f7fa", border: 1, borderColor: "divider", borderRadius: 1, fontSize: 12, lineHeight: 1.55, whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
              {job.query}
            </Box>
          </Box>
        </>
      )}
      {job.error && (
        <Box sx={{ px: 2 }}><ErrorView error={new Error(job.error)} /></Box>
      )}
      {job.state !== "DONE" && (
        <Box sx={{ p: 2 }}><Button color="error" startIcon={<Square size={14} />} onClick={onCancel}>Cancel job</Button></Box>
      )}
    </Box>
  );
}

export function JobsWorkspace({ projectId }: { projectId?: string }) {
  const api = useApi();
  const queryClient = useQueryClient();
  const theme = useTheme();
  const wide = useMediaQuery(theme.breakpoints.up("lg"));
  const [search, setSearch] = useState("");
  const [type, setType] = useState("ALL");
  const [state, setState] = useState("ALL");
  const [selected, setSelected] = useState<Job>();
  const jobs = useQuery({ queryKey: ["jobs", projectId], queryFn: () => api.listJobs(projectId!), enabled: Boolean(projectId) });
  const cancel = useMutation({
    mutationFn: (job: Job) => api.cancelJob(job.projectId, job.id),
    onSuccess: (job) => {
      setSelected(job);
      queryClient.invalidateQueries({ queryKey: ["jobs", projectId] });
    }
  });
  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase();
    return (jobs.data || []).filter((job) => {
      const status = job.error ? "FAILED" : job.state;
      return (
        (type === "ALL" || job.type === type) &&
        (state === "ALL" || status === state) &&
        (!needle || job.id.toLowerCase().includes(needle) || job.query?.toLowerCase().includes(needle))
      );
    });
  }, [jobs.data, search, state, type]);

  return (
    <Box sx={{ height: "100%", minHeight: 560, display: "flex", flexDirection: wide ? "row" : "column", bgcolor: "background.paper" }}>
      <Box sx={{ minWidth: 0, flex: 1, display: "flex", flexDirection: "column" }}>
        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ p: 2, alignItems: { sm: "center" }, borderBottom: 1, borderColor: "divider" }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="h1">Jobs</Typography>
            <Typography variant="body2" color="text.secondary">{projectId || "No project selected"}</Typography>
          </Box>
          <TextField
            size="small"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search jobs"
            slotProps={{ input: { startAdornment: <InputAdornment position="start"><Search size={16} /></InputAdornment> } }}
            sx={{ width: { xs: "100%", sm: 230 } }}
          />
          <FormControl size="small" sx={{ minWidth: 112 }}>
            <Select value={type} onChange={(event) => setType(event.target.value)} aria-label="Job type">
              {['ALL', 'QUERY', 'LOAD', 'EXTRACT', 'COPY'].map((value) => <MenuItem key={value} value={value}>{value === 'ALL' ? 'All types' : value}</MenuItem>)}
            </Select>
          </FormControl>
          <FormControl size="small" sx={{ minWidth: 120 }}>
            <Select value={state} onChange={(event) => setState(event.target.value)} aria-label="Job state">
              {['ALL', 'PENDING', 'RUNNING', 'DONE', 'FAILED'].map((value) => <MenuItem key={value} value={value}>{value === 'ALL' ? 'All states' : value}</MenuItem>)}
            </Select>
          </FormControl>
          <Tooltip title="Refresh"><IconButton onClick={() => jobs.refetch()} aria-label="Refresh jobs"><RefreshCw size={17} /></IconButton></Tooltip>
        </Stack>
        <Box sx={{ flex: 1, minHeight: 0, overflow: "auto" }}>
          {!projectId && <EmptyView title="Select a project" />}
          {jobs.isLoading && <LoadingView label="Loading jobs" />}
          {jobs.isError && <ErrorView error={jobs.error} onRetry={() => jobs.refetch()} />}
          {jobs.isSuccess && filtered.length === 0 && <EmptyView title="No matching jobs" />}
          {filtered.length > 0 && (
            <TableContainer sx={{ maxHeight: "100%" }}>
              <Table stickyHeader size="small" aria-label="Jobs">
                <TableHead><TableRow><TableCell>Status</TableCell><TableCell>Job ID</TableCell><TableCell>Type</TableCell><TableCell>Created</TableCell><TableCell>Duration</TableCell><TableCell>Processed</TableCell></TableRow></TableHead>
                <TableBody>
                  {filtered.map((job) => (
                    <TableRow key={job.id} hover selected={selected?.id === job.id} onClick={() => setSelected(job)} sx={{ cursor: "pointer" }}>
                      <TableCell><StatusChip job={job} /></TableCell>
                      <TableCell sx={{ fontFamily: "monospace", maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis" }}>{job.id}</TableCell>
                      <TableCell>{job.type}</TableCell>
                      <TableCell>{formatTime(job.createdAt)}</TableCell>
                      <TableCell>{formatDuration(job.startedAt, job.endedAt)}</TableCell>
                      <TableCell>{formatBytes(job.bytesProcessed)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          )}
        </Box>
      </Box>
      {selected && <JobDetails job={selected} onClose={() => setSelected(undefined)} onCancel={() => cancel.mutate(selected)} />}
    </Box>
  );
}
