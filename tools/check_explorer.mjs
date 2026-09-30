import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const explorerPath = path.join(here, "BatStore-Explorer.html");
const html = fs.readFileSync(explorerPath, "utf8");
const failures = [];

const sourceDir = path.join(here, "explorer-src");
const generated = fs.readFileSync(path.join(sourceDir, "shell.html"), "utf8")
  .replace("<!-- EXPLORER_STYLE -->", `<style>\n${fs.readFileSync(path.join(sourceDir, "styles.css"), "utf8").trimEnd()}\n</style>`)
  .replace("<!-- SNAPSHOT_SCRIPT -->", `<script>\n${fs.readFileSync(path.join(here, "batstore-snapshot.js"), "utf8").trimEnd()}\n</script>`)
  .replace("<!-- APP_SCRIPT -->", `<script>\n${fs.readFileSync(path.join(sourceDir, "app.js"), "utf8").trimEnd()}\n</script>`);
if (generated !== html) failures.push("generated Explorer is stale; run node tools/build_explorer.mjs");

const scripts = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(match => match[1]);
if (scripts.length !== 2) failures.push(`expected 2 embedded scripts, found ${scripts.length}`);
scripts.forEach((source, index) => {
  try { new Function(source); }
  catch (error) { failures.push(`embedded script ${index + 1} does not parse: ${error.message}`); }
});

const staticMarkup = html.replace(/<style>[\s\S]*?<\/style>/gi, "").replace(/<script(?:\s[^>]*)?>[\s\S]*?<\/script>/gi, "");
const staticIds = [...staticMarkup.matchAll(/\sid="([^"]+)"/g)].map(match => match[1]);
const duplicateIds = [...new Set(staticIds.filter((id, index) => staticIds.indexOf(id) !== index))];
if (duplicateIds.length) failures.push(`duplicate static ids: ${duplicateIds.join(", ")}`);

const requiredIds = [
  "workspaceTabs", "dashboardView", "dataView", "svg", "statusBar",
  "commandPalette", "rootSelector", "treeLayoutSelect", "utilityPanel",
  "tracePanel", "keyTracerPanel", "map2dPanel"
];
requiredIds.forEach(id => { if (!staticIds.includes(id)) failures.push(`missing required control #${id}`); });

const requiredFeatures = [
  ["middle-click transaction close", "onauxclick"],
  ["transaction close undo", 'label:"Undo"'],
  ["reduced-motion support", "prefers-reduced-motion"],
  ["command palette shortcut", 'e.key.toLowerCase() === "k"'],
  ["shared page header", "pageHeaderHtml"]
];
requiredFeatures.forEach(([label, marker]) => { if (!html.includes(marker)) failures.push(`missing ${label}`); });

if (failures.length) {
  console.error(failures.map(failure => `- ${failure}`).join("\n"));
  process.exit(1);
}

console.log(`Explorer OK: ${scripts.length} scripts, ${staticIds.length} static controls, ${html.length.toLocaleString()} bytes`);
