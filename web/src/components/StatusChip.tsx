import { Chip } from "@mui/material";
import { CircleCheck, CircleDashed, CircleX, LoaderCircle } from "lucide-react";
import type { Job } from "../domain/models";

export function StatusChip({ job }: { job: Job }) {
  if (job.error) {
    return <Chip size="small" color="error" variant="outlined" icon={<CircleX size={14} />} label="Failed" />;
  }
  if (job.state === "DONE") {
    return <Chip size="small" color="success" variant="outlined" icon={<CircleCheck size={14} />} label="Succeeded" />;
  }
  if (job.state === "RUNNING") {
    return <Chip size="small" color="primary" variant="outlined" icon={<LoaderCircle size={14} />} label="Running" />;
  }
  return <Chip size="small" variant="outlined" icon={<CircleDashed size={14} />} label="Pending" />;
}
