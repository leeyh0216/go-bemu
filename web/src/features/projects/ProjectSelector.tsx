import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  IconButton,
  List,
  ListItem,
  ListItemText,
  MenuItem,
  Select,
  Stack,
  TextField,
  Tooltip,
  Typography
} from "@mui/material";
import { FolderCog, Plus, Trash2 } from "lucide-react";
import { useApi } from "../../context/ApiContext";
import { ErrorView, LoadingView } from "../../components/StateViews";

export function ProjectSelector({ value, onChange }: { value?: string; onChange: (projectId: string) => void }) {
  const api = useApi();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [projectId, setProjectId] = useState("");
  const [projectName, setProjectName] = useState("");
  const projects = useQuery({ queryKey: ["projects"], queryFn: () => api.listProjects() });

  const sorted = useMemo(
    () => [...(projects.data || [])].sort((left, right) => left.name.localeCompare(right.name)),
    [projects.data]
  );

  const create = useMutation({
    mutationFn: () => api.createProject(projectId.trim(), projectName.trim() || projectId.trim()),
    onSuccess: (project) => {
      queryClient.invalidateQueries({ queryKey: ["projects"] });
      onChange(project.id);
      setProjectId("");
      setProjectName("");
    }
  });

  const remove = useMutation({
    mutationFn: (id: string) => api.deleteProject(id),
    onSuccess: (_, id) => {
      queryClient.invalidateQueries({ queryKey: ["projects"] });
      if (value === id) onChange(sorted.find((project) => project.id !== id)?.id || "");
    }
  });

  return (
    <>
      <Select
        size="small"
        value={value || ""}
        onChange={(event) => onChange(event.target.value)}
        displayEmpty
        aria-label="Active project"
        sx={{ minWidth: { xs: 150, sm: 230 }, maxWidth: 280, bgcolor: "background.paper" }}
      >
        {sorted.map((project) => (
          <MenuItem key={project.id} value={project.id}>
            <Box sx={{ minWidth: 0 }}>
              <Typography variant="body2" noWrap>{project.name}</Typography>
              <Typography variant="caption" color="text.secondary" noWrap>{project.id}</Typography>
            </Box>
          </MenuItem>
        ))}
        <Divider />
        <MenuItem onClick={() => setOpen(true)}>
          <FolderCog size={16} style={{ marginRight: 8 }} /> Manage projects
        </MenuItem>
      </Select>

      <Dialog open={open} onClose={() => setOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>Projects</DialogTitle>
        <DialogContent dividers sx={{ p: 0 }}>
          {projects.isLoading && <LoadingView label="Loading projects" />}
          {projects.isError && <ErrorView error={projects.error} onRetry={() => projects.refetch()} />}
          {projects.isSuccess && (
            <List disablePadding>
              {sorted.map((project) => (
                <ListItem
                  key={project.id}
                  divider
                  secondaryAction={
                    <Tooltip title="Delete project">
                      <IconButton
                        edge="end"
                        size="small"
                        aria-label={`Delete ${project.name}`}
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(project.id)}
                      >
                        <Trash2 size={16} />
                      </IconButton>
                    </Tooltip>
                  }
                >
                  <ListItemText primary={project.name} secondary={project.id} />
                </ListItem>
              ))}
            </List>
          )}
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ p: 2 }}>
            <TextField
              size="small"
              label="Project ID"
              value={projectId}
              onChange={(event) => setProjectId(event.target.value)}
              fullWidth
              slotProps={{ htmlInput: { pattern: "[a-z][a-z0-9-]{4,28}[a-z0-9]" } }}
            />
            <TextField
              size="small"
              label="Name"
              value={projectName}
              onChange={(event) => setProjectName(event.target.value)}
              fullWidth
            />
            <Button
              variant="contained"
              startIcon={<Plus size={16} />}
              disabled={!projectId.trim() || create.isPending}
              onClick={() => create.mutate()}
              sx={{ flexShrink: 0 }}
            >
              Create
            </Button>
          </Stack>
          {(create.error || remove.error) && <ErrorView error={create.error || remove.error} />}
        </DialogContent>
        <DialogActions><Button onClick={() => setOpen(false)}>Close</Button></DialogActions>
      </Dialog>
    </>
  );
}
