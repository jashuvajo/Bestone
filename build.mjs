import fs from "node:fs";
import { buildSync } from "esbuild";

fs.mkdirSync("dist/assets", { recursive: true });

buildSync({
  stdin: {
    contents: `import React from 'react'; import { createRoot } from 'react-dom/client'; import App from './App.jsx'; createRoot(document.getElementById('root')).render(React.createElement(App));`,
    resolveDir: process.cwd(),
    sourcefile: "entry.jsx",
    loader: "jsx",
  },
  bundle: true,
  minify: true,
  format: "esm",
  outfile: "dist/assets/app.js",
  define: {
    "import.meta.env.VITE_API_URL": JSON.stringify(process.env.VITE_API_URL || "http://localhost:8000"),
    "import.meta.env.VITE_WS_URL": JSON.stringify(process.env.VITE_WS_URL || "ws://localhost:8000/ws/dashboard"),
  },
});

fs.copyFileSync("index.html", "dist/index.html");
