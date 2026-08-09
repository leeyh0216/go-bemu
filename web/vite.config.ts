import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  return {
    base: env.VITE_BASE_PATH || "/console/",
    plugins: [react()],
    server: {
      port: 4173,
      strictPort: true,
      proxy: {
        "/bigquery": env.VITE_API_TARGET || "http://localhost:9050",
        "/emulator": env.VITE_API_TARGET || "http://localhost:9050"
      }
    },
    test: {
      environment: "jsdom",
      setupFiles: "./src/test/setup.ts",
      exclude: ["e2e/**", "node_modules/**", "dist/**"],
      css: true
    }
  };
});
