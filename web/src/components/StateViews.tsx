import { Alert, Box, Button, CircularProgress, Typography } from "@mui/material";
import { Database, RefreshCw } from "lucide-react";

export function LoadingView({ label = "Loading" }: { label?: string }) {
  return (
    <Box sx={{ minHeight: 180, display: "grid", placeItems: "center", color: "text.secondary" }} role="status">
      <Box sx={{ display: "flex", alignItems: "center", gap: 1.25 }}>
        <CircularProgress size={18} />
        <Typography variant="body2">{label}</Typography>
      </Box>
    </Box>
  );
}

export function EmptyView({ title, detail, action }: { title: string; detail?: string; action?: React.ReactNode }) {
  return (
    <Box sx={{ minHeight: 220, display: "grid", placeItems: "center", textAlign: "center", px: 3 }}>
      <Box>
        <Database size={28} strokeWidth={1.5} color="#6b7280" />
        <Typography variant="h3" sx={{ mt: 1 }}>{title}</Typography>
        {detail && <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>{detail}</Typography>}
        {action && <Box sx={{ mt: 1.5 }}>{action}</Box>}
      </Box>
    </Box>
  );
}

export function ErrorView({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const message = error instanceof Error ? error.message : "The request failed";
  return (
    <Alert
      severity="error"
      action={
        onRetry && (
          <Button color="inherit" size="small" startIcon={<RefreshCw size={15} />} onClick={onRetry}>
            Retry
          </Button>
        )
      }
      sx={{ m: 2 }}
    >
      {message}
    </Alert>
  );
}
