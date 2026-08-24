import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { execFileSync } from "node:child_process";

function gitValue(args: string[], fallback: string) {
  try {
    return execFileSync("git", args, {
      cwd: new URL("..", import.meta.url),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim() || fallback;
  } catch {
    return fallback;
  }
}

const commitId = process.env.VITE_BUILD_COMMIT_ID
  || gitValue(["rev-parse", "HEAD"], "unknown");
const commitTime = process.env.VITE_BUILD_COMMIT_TIME
  || gitValue(["show", "-s", "--format=%cI", "HEAD"], "unknown");
const branch = process.env.VITE_BUILD_BRANCH
  || process.env.GITHUB_HEAD_REF
  || process.env.GITHUB_REF_NAME
  || gitValue(["branch", "--show-current"], "detached");
const apiProxyTarget = process.env.VITE_API_PROXY_TARGET || "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/health": {
        target: apiProxyTarget,
        changeOrigin: true,
      },
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
  define: {
    __BUILD_COMMIT_ID__: JSON.stringify(commitId),
    __BUILD_COMMIT_TIME__: JSON.stringify(commitTime),
    __BUILD_BRANCH__: JSON.stringify(branch),
  },
});
