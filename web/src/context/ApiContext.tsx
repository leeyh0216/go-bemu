import { createContext, type PropsWithChildren, useContext } from "react";
import type { BigQueryApi } from "../ports/BigQueryApi";

const ApiContext = createContext<BigQueryApi | null>(null);

export function ApiProvider({ api, children }: PropsWithChildren<{ api: BigQueryApi }>) {
  return <ApiContext.Provider value={api}>{children}</ApiContext.Provider>;
}

export function useApi(): BigQueryApi {
  const api = useContext(ApiContext);
  if (!api) throw new Error("ApiProvider is missing");
  return api;
}
