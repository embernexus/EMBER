import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const apiTarget = String(env.VITE_API_BASE_URL || `http://localhost:${env.PORT || 3002}`).replace(
    /\/+$/,
    ""
  );

  return {
    root: "web",
    plugins: [react()],
    server: {
      port: 5173,
      proxy: {
        "/api": {
          target: apiTarget,
          changeOrigin: true,
        },
      },
    },
    build: {
      outDir: "../dist/web",
      emptyOutDir: true,
    },
  };
});
