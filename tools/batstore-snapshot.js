/* Read-only snapshot model for Explorer dumps. No engine state is changed. */
(function (scope) {
  "use strict";
  const bigint = value => { try { return BigInt(value); } catch { return null; } };
  const compareKeys = (a, b) => {
    const x = bigint(a), y = bigint(b);
    return x !== null && y !== null ? (x < y ? -1 : x > y ? 1 : 0)
      : String(a).localeCompare(String(b), undefined, {numeric:true});
  };
  function clockInfo(tree, bundle) {
    let observed = 0n, worker = 0;
    for (const root of tree.roots || []) observed = observed > BigInt(root.version) ? observed : BigInt(root.version);
    for (const node of Object.values(tree.nodes || {})) {
      for (const child of node.children || []) {
        const v = bigint(child.version); if (v !== null && v > observed) observed = v;
      }
      for (const record of node.records || []) {
        worker = Math.max(worker, Number(record.insert_worker || 0), Number(record.delete_worker || 0));
        for (const stamp of [record.insert_ts, record.delete_ts]) {
          const v = bigint(stamp); if (v !== null && v > observed) observed = v;
        }
      }
    }
    const next = bigint(tree.glc_next ?? bundle?.glc_next);
    const explicitLast = bigint(tree.glc_last ?? bundle?.glc_last);
    return {last: explicitLast ?? (next === null ? observed : next > 0n ? next - 1n : 0n),
      next: next === null ? observed + 1n : next,
      maxWorker: Number(tree.max_worker_id ?? bundle?.max_worker_id ?? worker),
      exact: Array.isArray(tree.commit_logs) && tree.historical_visibility_complete === true,
      inferredClock: explicitLast === null && next === null};
  }
  function overlaps(a, b) {
    return compareKeys(a.key_lower, b.key_upper) <= 0 && compareKeys(b.key_lower, a.key_upper) <= 0;
  }
  function activeChildren(node, ts) {
    const eligible = (node.children || []).filter(child => {
      const stamp = bigint(child.version); return stamp !== null && stamp <= ts;
    });
    return eligible.filter((child, index) => !eligible.slice(index + 1).some(later => overlaps(child, later)));
  }
  function rootAt(tree, ts) {
    const roots = [...(tree.roots || [])].sort((a,b) => bigint(a.version) < bigint(b.version) ? -1 : 1);
    if (!roots.length) return null;
    let result = roots[0];
    for (const root of roots) { if (bigint(root.version) <= ts) result = root; else break; }
    return result;
  }
  function stampVisible(tree, ts, readerWorker, stampWorker, stampTs, invalid) {
    if (invalid) return false;
    const stamp = bigint(stampTs);
    if (stamp === null || stamp > ts) return false;
    if (Number(stampWorker) === readerWorker) return true;
    const log = tree.commit_logs?.[Number(stampWorker)];
    if (!Array.isArray(log)) return stamp < ts; // legacy dump: commit unknown
    let last = 0n;
    for (const entry of log) {
      const commit = bigint(entry);
      if (commit !== null && commit < ts && commit > last) last = commit;
    }
    return last > stamp;
  }
  function progressTable(tree, tsInput, maxWorker, simulatedCommits = []) {
    const ts = bigint(tsInput);
    if (ts === null || ts < 0n) throw new Error("Snapshot time must be a nonnegative integer.");
    const count = Math.max(0, Number(maxWorker ?? clockInfo(tree).maxWorker));
    const complete = Array.isArray(tree.commit_logs) && tree.historical_visibility_complete === true;
    const entries = [];
    for (let worker = 0; worker <= count; worker++) {
      let last = 0n;
      for (const value of tree.commit_logs?.[worker] || []) {
        const commit = bigint(value);
        if (commit !== null && commit < ts && commit > last) last = commit;
      }
      for (const entry of simulatedCommits) {
        if (Number(entry.worker) !== worker) continue;
        const commit = bigint(entry.commit);
        if (commit !== null && commit < ts && commit > last) last = commit;
      }
      entries.push({worker, lcb:complete || last > 0n ? String(last) : null, exact:complete});
    }
    return {ts:String(ts),entries,exact:complete};
  }
  function workerActivity(tree) {
    const clock = clockInfo(tree), workers = Array.from({length:clock.maxWorker+1}, (_,worker) => ({
      worker,commits:clock.exact ? (tree.commit_logs?.[worker]?.length || 0) : null,
      writes:0,deletes:0
    }));
    const writes = new Set(), deletes = new Set();
    for (const node of Object.values(tree.nodes || {})) {
      if (node.type !== "leaf") continue;
      for (const record of node.records || []) {
        if (!record.insert_invalid) {
          const id=[record.key,record.insert_worker,record.insert_ts].join("\u0000");
          if (!writes.has(id)) {writes.add(id);if(workers[record.insert_worker])workers[record.insert_worker].writes++;}
        }
        if (record.deleted && !record.delete_invalid) {
          const id=[record.key,record.delete_worker,record.delete_ts].join("\u0000");
          if (!deletes.has(id)) {deletes.add(id);if(workers[record.delete_worker])workers[record.delete_worker].deletes++;}
        }
      }
    }
    return {exactCommits:clock.exact,workers};
  }
  function snapshot(tree, tsInput, readerWorker, bundleRows, rowsVersion, options = {}) {
    const ts = bigint(tsInput);
    if (ts === null || ts < 0n) throw new Error("Snapshot time must be a nonnegative integer.");
    const root = rootAt(tree, ts), found = new Map(), phantoms = new Map(), visited = new Set();
    if (!root) return {rows:[], phantoms:[], root:null, truncated:false};
    let truncated = ts > 0n && ts < bigint(root.version);
    const visibilityTs = () => {
      if (typeof options.nextVisibilityTs !== "function") return ts;
      const refreshed = bigint(options.nextVisibilityTs());
      return refreshed === null ? ts : refreshed;
    };
    function walk(id) {
      if (visited.has(id)) return;
      visited.add(id);
      const node = tree.nodes?.[id]; if (!node) {truncated=true;return;}
      if (node.type === "internal_truncated") {truncated=true;return;}
      if (node.type === "internal") {for (const child of activeChildren(node, ts)) walk(child.node_id);return;}
      for (const record of node.records || []) {
        const key = String(record.key);
        const metadata = {key,insert_ts:String(record.insert_ts),insert_worker:record.insert_worker,
          delete_ts:record.deleted ? String(record.delete_ts) : null,
          delete_worker:record.deleted ? record.delete_worker : null,
          status:record.insert_invalid ? "PHANTOM" : record.delete_invalid ? "ABORTED DELETE" : "VISIBLE"};
        if (record.insert_invalid) {
          const stamp = bigint(record.insert_ts);
          if (stamp !== null && stamp <= ts) phantoms.set(key+":"+record.insert_worker+":"+record.insert_ts,metadata);
          continue;
        }
        if (!stampVisible(tree, visibilityTs(), readerWorker, record.insert_worker, record.insert_ts, record.insert_invalid)) continue;
        if (record.deleted && stampVisible(tree, visibilityTs(), readerWorker, record.delete_worker, record.delete_ts, record.delete_invalid)) continue;
        const previous = found.get(key);
        if (!previous || bigint(record.insert_ts) > bigint(previous.insert_ts)) found.set(key,metadata);
      }
    }
    walk(root.node_id);
    const valuesAvailable = bundleRows && rowsVersion != null && ts >= bigint(rowsVersion)
      && ts <= clockInfo(tree).last + 1n;
    const values = valuesAvailable ? new Map(bundleRows.map(row => [String(row.key), row])) : null;
    const rows = [...found.values()].map(row => values?.has(row.key) ? {...values.get(row.key),...row} : row)
      .sort((a,b) => compareKeys(a.key,b.key));
    return {rows,phantoms:[...phantoms.values()].sort((a,b)=>compareKeys(a.key,b.key)),root,truncated,valuesAvailable:!!valuesAvailable};
  }
  function physicalSnapshot(tree, bundleRows) {
    const ts=clockInfo(tree).last,root=rootAt(tree,ts),found=new Map(),visited=new Set();
    if(!root)return {rows:[],root:null,truncated:false,valuesAvailable:false};
    let truncated=false;
    function walk(id){
      if(visited.has(id))return;visited.add(id);
      const node=tree.nodes?.[id];if(!node){truncated=true;return;}
      if(node.type==="internal_truncated"){truncated=true;return;}
      if(node.type==="internal"){for(const child of activeChildren(node,ts))walk(child.node_id);return;}
      for(const record of node.records||[]){
        const key=String(record.key),previous=found.get(key),stamp=bigint(record.insert_ts)??0n;
        if(previous&&stamp<previous._physicalStamp)continue;
        found.set(key,{key,insert_ts:String(record.insert_ts),insert_worker:record.insert_worker,
          delete_ts:record.deleted?String(record.delete_ts):null,delete_worker:record.deleted?record.delete_worker:null,
          status:"PHYSICALLY ACCESSED",_physicalStamp:stamp});
      }
    }
    walk(root.node_id);
    const values=new Map((bundleRows||[]).map(row=>[String(row.key),row]));
    const rows=[...found.values()].map(row=>{const {_physicalStamp,...clean}=row;return values.has(clean.key)?{...values.get(clean.key),...clean}:clean;}).sort((a,b)=>compareKeys(a.key,b.key));
    return {rows,root,truncated,valuesAvailable:values.size>0};
  }
  function select(rows, {key, lower, upper, field="key", operator="contains", value=""}={}) {
    return rows.filter(row => {
      if (key != null && String(row.key) !== String(key)) return false;
      if (lower != null && compareKeys(row.key,lower) < 0) return false;
      if (upper != null && compareKeys(row.key,upper) > 0) return false;
      if (value === "") return true;
      const actual = row[field] == null ? "" : String(row[field]);
      const cmp = compareKeys(actual,value);
      return operator === "eq" ? cmp === 0 : operator === "gte" ? cmp >= 0
        : operator === "lte" ? cmp <= 0 : actual.toLowerCase().includes(String(value).toLowerCase());
    });
  }
  function execute(tables, operations, ts, worker, rowsVersion, options = {}) {
    const mode=options.mode||"si",dynamic=mode==="rc"||mode==="ru",overlays=tables.map(()=>new Map());
    let glcCursor=bigint(options.glcBase)??bigint(ts)??0n,visibilityChecks=0;
    if(mode==="rc")glcCursor+=1n; // BEGIN draws the RC transaction's ts_start.
    const statementSnapshots=[],statementVisibilityChecks=[];
    const loadState=(table,index,statementTs)=>{
      const nextVisibilityTs=options.reloadGlcPerTuple?()=>{visibilityChecks++;return glcCursor;}:null;
      const rows=mode==="ru"?physicalSnapshot(table.tree,table.rows).rows:snapshot(table.tree,statementTs,worker,table.rows,table.snapshot_version??rowsVersion,{nextVisibilityTs}).rows;
      const map=new Map(rows.map(row=>[String(row.key),{...row}]));
      for(const [key,row] of overlays[index])row===null?map.delete(key):map.set(key,{...row});
      return map;
    };
    const state=dynamic?tables.map(()=>null):tables.map((table,index)=>loadState(table,index,ts));
    const steps = [];
    let aborted = false;
    for (const op of operations) {
      let statementTs=bigint(ts)??0n;
      if(mode==="rc")statementTs=glcCursor;
      statementSnapshots.push(String(statementTs));
      const checksBefore=visibilityChecks;
      const map=dynamic?loadState(tables[op.tableIndex],op.tableIndex,statementTs):state[op.tableIndex];
      statementVisibilityChecks.push(visibilityChecks-checksBefore);
      if (!map) throw new Error("Unknown table in transaction.");
      const before = map.get(String(op.key));
      let result;
      if (op.kind === "read") result = before ? {status:"found",rows:[{...before}]} : {status:"missing",rows:[]};
      else if (op.kind === "scan") {
        const rows = [...map.values()].filter(row => compareKeys(row.key,op.lower)>=0 && compareKeys(row.key,op.upper)<=0).sort((a,b)=>compareKeys(a.key,b.key));
        result = {status:"matched",rows};
      } else if (op.kind === "insert") {
        if (before) { result={status:"conflict",rows:[{...before}]};aborted=true; }
        else {const row={key:String(op.key),...(op.values||{})};map.set(String(op.key),row);overlays[op.tableIndex].set(String(op.key),row);result={status:"inserted",rows:[{...row}]};}
      } else if (op.kind === "update") {
        if (!before) {result={status:"missing",rows:[]};aborted=true;}
        else {const row={...before,...(op.values||{})};map.set(String(op.key),row);overlays[op.tableIndex].set(String(op.key),row);result={status:"updated",rows:[{...row}]};}
      } else if (op.kind === "delete") {
        if (!before) {result={status:"missing",rows:[]};aborted=true;}
        else {map.delete(String(op.key));overlays[op.tableIndex].set(String(op.key),null);result={status:"deleted",rows:[{...before}]};}
      } else throw new Error("Unknown operation: "+op.kind);
      steps.push({operation:op,result});
      if(mode==="rc"&&["inserted","updated","deleted"].includes(result.status))glcCursor+=1n;
      if (aborted) break;
    }
    return {steps,aborted,statementSnapshots,statementVisibilityChecks,visibilityChecks,lastGlc:String(glcCursor)};
  }
  const api = {clockInfo,progressTable,workerActivity,snapshot,physicalSnapshot,select,execute,compareKeys};
  scope.BatStoreSnapshot = api;
  if (typeof module !== "undefined" && module.exports) module.exports = api;
})(typeof window !== "undefined" ? window : globalThis);
