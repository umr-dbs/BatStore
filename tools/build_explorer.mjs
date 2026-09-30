import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const toolsDir = path.dirname(fileURLToPath(import.meta.url));
const sourceDir = path.join(toolsDir, "explorer-src");
const outputPath = path.join(toolsDir, "BatStore-Explorer.html");
const shellPath = path.join(sourceDir, "shell.html");
const stylePath = path.join(sourceDir, "styles.css");
const appPath = path.join(sourceDir, "app.js");
const snapshotPath = path.join(toolsDir, "batstore-snapshot.js");

const styleMarker = "<!-- EXPLORER_STYLE -->";
const snapshotMarker = "<!-- SNAPSHOT_SCRIPT -->";
const appMarker = "<!-- APP_SCRIPT -->";

function extract() {
  const html = fs.readFileSync(outputPath, "utf8");
  const styleMatch = html.match(/<style>\n([\s\S]*?)\n<\/style>/);
  const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>\n([\s\S]*?)\n<\/script>/gi)];
  if (!styleMatch || scripts.length !== 2) throw new Error("Explorer structure changed; expected one style and two script blocks.");

  fs.mkdirSync(sourceDir, { recursive: true });
  fs.writeFileSync(stylePath, styleMatch[1] + "\n");
  fs.writeFileSync(appPath, scripts[1][1] + "\n");

  let shell = html.replace(styleMatch[0], styleMarker);
  shell = shell.replace(scripts[0][0], snapshotMarker).replace(scripts[1][0], appMarker);
  fs.writeFileSync(shellPath, shell);
  console.log("Extracted Explorer shell, styles, and application logic.");
}

function build() {
  let html = fs.readFileSync(shellPath, "utf8");
  const style = fs.readFileSync(stylePath, "utf8").trimEnd();
  const snapshot = fs.readFileSync(snapshotPath, "utf8").trimEnd();
  const app = fs.readFileSync(appPath, "utf8").trimEnd();
  html = html
    .replace(styleMarker, `<style>\n${style}\n</style>`)
    .replace(snapshotMarker, `<script>\n${snapshot}\n</script>`)
    .replace(appMarker, `<script>\n${app}\n</script>`);
  if (html.includes(styleMarker) || html.includes(snapshotMarker) || html.includes(appMarker)) throw new Error("An Explorer build marker was not replaced.");
  fs.writeFileSync(outputPath, html);
  console.log(`Built ${path.relative(process.cwd(), outputPath)} (${html.length.toLocaleString()} bytes).`);
}

const command = process.argv[2] || "build";
if (command === "extract") extract();
else if (command === "build") build();
else throw new Error("Usage: node tools/build_explorer.mjs [build|extract]");
