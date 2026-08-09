import { type PropsWithChildren, useEffect, useState } from "react";
import {
  AppBar,
  Box,
  Button,
  Drawer,
  IconButton,
  Stack,
  Toolbar,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme
} from "@mui/material";
import { BriefcaseBusiness, DatabaseZap, Menu, PanelLeftClose, PanelLeftOpen, Play, X } from "lucide-react";
import { ProjectSelector } from "../features/projects/ProjectSelector";

const explorerWidth = 292;

export function AppShell({
  projectId,
  onProjectChange,
  activeView,
  navigationKey,
  onNavigate,
  explorer,
  children
}: PropsWithChildren<{
  projectId?: string;
  onProjectChange: (projectId: string) => void;
  activeView: "query" | "jobs" | "table";
  navigationKey: string;
  onNavigate: (view: "query" | "jobs") => void;
  explorer: React.ReactNode;
}>) {
  const theme = useTheme();
  const desktop = useMediaQuery(theme.breakpoints.up("md"));
  const [desktopExplorerOpen, setDesktopExplorerOpen] = useState(true);
  const [mobileExplorerOpen, setMobileExplorerOpen] = useState(false);
  const visible = desktop && desktopExplorerOpen;

  useEffect(() => {
    setMobileExplorerOpen(false);
  }, [navigationKey]);

  const toggleExplorer = () => {
    if (desktop) {
      setDesktopExplorerOpen((current) => !current);
      return;
    }
    setMobileExplorerOpen((current) => !current);
  };

  return (
    <Box sx={{ height: "100dvh", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <AppBar position="static" color="inherit" elevation={0} sx={{ borderBottom: 1, borderColor: "divider", zIndex: 1301 }}>
        <Toolbar variant="dense" sx={{ minHeight: 56, gap: 1.25, px: { xs: 1, sm: 2 } }}>
          <Tooltip title={visible ? "Hide explorer" : "Show explorer"}>
            <IconButton
              aria-label={visible ? "Hide explorer" : "Show explorer"}
              onClick={toggleExplorer}
            >
              {visible ? <PanelLeftClose size={19} /> : desktop ? <PanelLeftOpen size={19} /> : <Menu size={19} />}
            </IconButton>
          </Tooltip>
          <DatabaseZap size={23} color={theme.palette.primary.main} />
          <Typography variant="h3" sx={{ display: { xs: "none", sm: "block" }, mr: 1, whiteSpace: "nowrap" }}>
            BQ Emulator
          </Typography>
          <ProjectSelector value={projectId} onChange={onProjectChange} />
          <Box sx={{ flex: 1 }} />
          <Stack direction="row" spacing={0.25}>
            <Button
              aria-label="Query"
              color={activeView === "query" ? "primary" : "inherit"}
              startIcon={<Play size={16} />}
              sx={{ minWidth: { xs: 40, sm: 82 } }}
              onClick={() => onNavigate("query")}
            >
              <Box component="span" sx={{ display: { xs: "none", sm: "inline" } }}>Query</Box>
            </Button>
            <Button
              aria-label="Jobs"
              color={activeView === "jobs" ? "primary" : "inherit"}
              startIcon={<BriefcaseBusiness size={16} />}
              sx={{ minWidth: { xs: 40, sm: 74 } }}
              onClick={() => onNavigate("jobs")}
            >
              <Box component="span" sx={{ display: { xs: "none", sm: "inline" } }}>Jobs</Box>
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>

      <Box sx={{ flex: 1, minHeight: 0, display: "flex" }}>
        {desktop && visible && (
          <Box component="aside" sx={{ width: explorerWidth, flexShrink: 0, borderRight: 1, borderColor: "divider" }}>
            {explorer}
          </Box>
        )}
        {!desktop && (
          <Drawer open={mobileExplorerOpen} onClose={() => setMobileExplorerOpen(false)} ModalProps={{ keepMounted: true }}>
            <Box sx={{ width: "min(88vw, 320px)", height: "100%" }}>
              <Box sx={{ height: 48, px: 1, display: "flex", alignItems: "center", justifyContent: "flex-end", borderBottom: 1, borderColor: "divider" }}>
                <IconButton onClick={() => setMobileExplorerOpen(false)} aria-label="Close explorer"><X size={18} /></IconButton>
              </Box>
              {explorer}
            </Box>
          </Drawer>
        )}
        <Box component="main" sx={{ flex: 1, minWidth: 0, minHeight: 0, overflow: "auto" }}>
          {children}
        </Box>
      </Box>
    </Box>
  );
}
