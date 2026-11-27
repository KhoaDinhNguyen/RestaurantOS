/// <reference types="vitest/config" />
import path from "node:path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig, loadEnv, type ProxyOptions } from "vite";

// The gateway, edge and coordinator send no CORS headers, so the dev server
// forwards every call (docs/work-packages/wp7-web-ui.md, "Reaching them from
// the browser"). The browser's Origin is passed on unchanged: the control
// plane checks it on every POST, PATCH and DELETE, so the page's origin must
// be in ROS_SIM_CONTROL_ALLOWED_ORIGINS (see README.md).
// The browser only ever uses relative paths:
//   /api/...          -> gateway       (8000)
//   /admin/...        -> coordinator   (8001)
//   /sim/...          -> control plane (8002)
//   /edge/r{n}/...    -> edge API of region n (8010 + n), prefix stripped
export default defineConfig(({ mode }) => {
  // Settings come from the repository's .env (and the real environment),
  // next to the backend's. Only ROS_UI_* keys are read.
  const env = loadEnv(mode, path.resolve(import.meta.dirname, ".."), "ROS_UI_");
  const host = env.ROS_UI_PROXY_HOST || "127.0.0.1";
  const regions = Number.parseInt(env.ROS_UI_REGIONS || "3", 10);
  const regionIds = Array.from({ length: regions }, (_, i) => `r${i + 1}`);

  const proxy: Record<string, ProxyOptions> = {
    "/api": { target: `http://${host}:8000` },
    "/admin": { target: `http://${host}:8001` },
    "/sim": { target: `http://${host}:8002` },
  };
  regionIds.forEach((regionId, i) => {
    const prefix = `/edge/${regionId}`;
    proxy[`${prefix}/`] = {
      target: `http://${host}:${8011 + i}`,
      rewrite: (p) => p.slice(prefix.length),
    };
  });

  return {
    plugins: [react(), tailwindcss()],
    define: {
      __ROS_REGIONS__: JSON.stringify(regionIds),
    },
    server: { proxy },
    preview: { proxy },
    test: {
      environment: "jsdom",
      setupFiles: ["./src/test/setup.ts"],
      css: false,
    },
  };
});
