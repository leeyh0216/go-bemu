import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { CssBaseline, ThemeProvider } from "@mui/material";
import App from "./App";
import { HttpBigQueryApi } from "./adapters/http/HttpBigQueryApi";
import { MockBigQueryApi } from "./adapters/mock/MockBigQueryApi";
import { ApiProvider } from "./context/ApiContext";
import { theme } from "./theme";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: 1, staleTime: 5000, refetchOnWindowFocus: false },
    mutations: { retry: false }
  }
});

const api = import.meta.env.VITE_USE_MOCK === "true"
  ? new MockBigQueryApi()
  : new HttpBigQueryApi(import.meta.env.VITE_API_BASE_URL || "");

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <QueryClientProvider client={queryClient}>
        <ApiProvider api={api}>
          <App />
        </ApiProvider>
      </QueryClientProvider>
    </ThemeProvider>
  </StrictMode>
);
