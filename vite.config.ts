import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");

  return {
    plugins: [react()],
    define: {
      "import.meta.env.API_URL": JSON.stringify(env.API_URL ?? ""),
      "import.meta.env.WS_URL": JSON.stringify(env.WS_URL ?? ""),
      "import.meta.env.DATA_ADAPTER": JSON.stringify(env.DATA_ADAPTER ?? ""),
      "import.meta.env.REALTIME_ADAPTER": JSON.stringify(env.REALTIME_ADAPTER ?? "")
    }
  };
});
