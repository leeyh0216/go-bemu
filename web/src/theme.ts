import { createTheme } from "@mui/material/styles";

export const theme = createTheme({
  palette: {
    mode: "light",
    primary: { main: "#1769e0", dark: "#0e4fae", light: "#d9e7ff" },
    secondary: { main: "#167b5b" },
    error: { main: "#c62828" },
    warning: { main: "#a35f00" },
    success: { main: "#18794e" },
    background: { default: "#f8fafd", paper: "#ffffff" },
    text: { primary: "#1f2937", secondary: "#5f6b7a" },
    divider: "#dfe3e8"
  },
  shape: { borderRadius: 6 },
  typography: {
    fontFamily: 'Inter, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
    h1: { fontSize: "1.5rem", fontWeight: 600, lineHeight: 1.3, letterSpacing: 0 },
    h2: { fontSize: "1.125rem", fontWeight: 600, lineHeight: 1.35, letterSpacing: 0 },
    h3: { fontSize: "0.95rem", fontWeight: 600, lineHeight: 1.4, letterSpacing: 0 },
    body1: { fontSize: "0.875rem", letterSpacing: 0 },
    body2: { fontSize: "0.8125rem", letterSpacing: 0 },
    button: { fontSize: "0.8125rem", fontWeight: 600, textTransform: "none", letterSpacing: 0 },
    caption: { fontSize: "0.75rem", letterSpacing: 0 }
  },
  components: {
    MuiButton: { defaultProps: { disableElevation: true }, styleOverrides: { root: { minHeight: 34 } } },
    MuiIconButton: { styleOverrides: { root: { borderRadius: 6 } } },
    MuiPaper: { styleOverrides: { root: { backgroundImage: "none" } } },
    MuiTableCell: {
      styleOverrides: {
        root: { borderColor: "#e7eaee", padding: "8px 12px", whiteSpace: "nowrap" },
        head: { backgroundColor: "#f5f7fa", color: "#44505f", fontWeight: 600 }
      }
    },
    MuiTooltip: { defaultProps: { arrow: true } },
    MuiDialog: { styleOverrides: { paper: { borderRadius: 8 } } },
    MuiChip: { styleOverrides: { root: { borderRadius: 4 } } }
  }
});
