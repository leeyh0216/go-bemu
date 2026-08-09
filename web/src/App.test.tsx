import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { CssBaseline, ThemeProvider } from "@mui/material";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import App from "./App";
import { MockBigQueryApi } from "./adapters/mock/MockBigQueryApi";
import { ApiProvider } from "./context/ApiContext";
import { theme } from "./theme";

function renderApp() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <QueryClientProvider client={client}>
        <ApiProvider api={new MockBigQueryApi()}><App /></ApiProvider>
      </QueryClientProvider>
    </ThemeProvider>
  );
}

describe("console workspace", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    localStorage.clear();
    window.location.hash = "/query";
  });

  it("runs a query and renders results", async () => {
    renderApp();
    expect(await screen.findByText("analytics")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Query" }));
    fireEvent.click(await screen.findByRole("button", { name: "Run" }));
    expect(await screen.findByText("page_view")).toBeInTheDocument();
    expect(screen.getByText("8421")).toBeInTheDocument();
  });

  it("opens table schema from the shared explorer", async () => {
    renderApp();
    fireEvent.click(await screen.findByText("events"));
    await waitFor(() => expect(window.location.hash).toContain("/table/analytics/events"));
    expect(await screen.findByText("event_id")).toBeInTheDocument();
    expect(screen.getByText("occurred_at")).toBeInTheDocument();
  });
});
