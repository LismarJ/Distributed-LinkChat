// Dev: `npm run dev` on :5173, with /api (HTTP and the WebSocket) proxied to the
// client API on :8000, or on LINKCHAT_API_PORT (scripts/dev_ui.py runs a second
// client on :8001). Build: written straight into the Python package, which
// serves it, so the container needs no Node.
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const apiPort = process.env.LINKCHAT_API_PORT ?? "8000";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": { target: `http://localhost:${apiPort}`, ws: true },
    },
  },
  build: {
    outDir: "../src/linkchat/client_app/static",
    emptyOutDir: true,
  },
});
