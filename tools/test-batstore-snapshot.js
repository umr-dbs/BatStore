const assert = require("node:assert/strict");
const model = require("./batstore-snapshot.js");
const out = require("./out.json");

const legacyClock = model.clockInfo(out);
assert.equal(legacyClock.last, 46859n);
assert.equal(legacyClock.exact, false);
assert.equal(model.snapshot(out, 0n, 0).rows.length, 0);
const legacySnapshot = model.snapshot(out, 100n, 0);
assert(legacySnapshot.rows.length > 0);
assert(model.select(legacySnapshot.rows, {key:"1"}).length > 0);
assert.equal(model.select(legacySnapshot.rows, {lower:"0",upper:"2"}).length,3);

const tree = {
  glc_next:"10", max_worker_id:2, historical_visibility_complete:true,
  commit_logs:[["3"],["7"],[]], roots:[{version:1,node_id:"leaf"}],
  nodes:{leaf:{type:"leaf",records:[
    {key:"42",insert_worker:0,insert_ts:2,insert_invalid:false,
      deleted:true,delete_worker:1,delete_ts:5,delete_invalid:false}
  ]}}
};
assert.equal(model.snapshot(tree,4n,2).rows.length,1);
assert.deepEqual(model.snapshot(tree,4n,2).rows[0],{
  key:"42",insert_ts:"2",insert_worker:0,delete_ts:"5",delete_worker:1,status:"VISIBLE"
});
assert.equal(model.snapshot(tree,6n,2).rows.length,1);
assert.equal(model.snapshot(tree,8n,2).rows.length,0);
assert.equal(model.snapshot(tree,2n,0).rows.length,1);
assert.deepEqual(model.progressTable(tree,7n,2).entries.map(entry=>entry.lcb),["3","0","0"]);
assert.deepEqual(model.progressTable(tree,8n,2,[{worker:2,commit:"7"}]).entries.map(entry=>entry.lcb),["3","7","7"]);
assert.equal(model.progressTable(out,100n,legacyClock.maxWorker).exact,false);
assert.deepEqual(model.workerActivity(tree).workers.map(worker=>[worker.commits,worker.writes,worker.deletes]),
  [[1,1,0],[1,0,1],[0,0,0]]);

const invalidTree={...tree,nodes:{leaf:{type:"leaf",records:[
  {key:"9",insert_worker:1,insert_ts:3,insert_invalid:true,deleted:false},
  {key:"10",insert_worker:0,insert_ts:2,insert_invalid:false,
    deleted:true,delete_worker:1,delete_ts:5,delete_invalid:true}
]}}};
const invalidSnapshot=model.snapshot(invalidTree,8n,2);
assert.deepEqual(invalidSnapshot.rows.map(row=>[row.key,row.status]),[["10","ABORTED DELETE"]]);
assert.deepEqual(invalidSnapshot.phantoms.map(row=>[row.key,row.status]),[["9","PHANTOM"]]);
assert.equal(model.select(invalidSnapshot.rows,{key:"9"}).length,0);

const cleanTree={...tree,nodes:{leaf:{type:"leaf",records:[]}}};
const operations=[
  {kind:"insert",tableIndex:0,key:"7",values:{value:"first"}},
  {kind:"read",tableIndex:0,key:"7"},
  {kind:"update",tableIndex:0,key:"7",values:{value:"second"}},
  {kind:"scan",tableIndex:0,lower:"1",upper:"9"},
  {kind:"delete",tableIndex:0,key:"7"},
  {kind:"read",tableIndex:0,key:"7"}
];
const result=model.execute([{name:"one",tree:cleanTree,rows:[]}],operations,10n,2);
assert.equal(result.aborted,false);
assert.deepEqual(result.steps.map(step=>step.result.status),
  ["inserted","found","updated","matched","deleted","missing"]);
assert.equal(result.steps[3].result.rows[0].value,"second");
const withPayload={name:"one",tree,rows:[{key:"42",value:"stored"}],snapshot_version:"4"};
assert.equal(model.execute([withPayload],[{kind:"read",tableIndex:0,key:"42"}],4n,2).steps[0].result.rows[0].value,"stored");
assert.equal(model.execute([withPayload],[{kind:"read",tableIndex:0,key:"42",predicate:{field:"value",operator:"eq",value:"stored"}}],4n,2).steps[0].result.status,"found");
assert.equal(model.execute([withPayload],[{kind:"read",tableIndex:0,key:"42",predicate:{field:"value",operator:"contains",value:"missing"}}],4n,2).steps[0].result.status,"filtered");
assert.deepEqual(model.execute([{name:"one",tree:cleanTree,rows:[]}],[{kind:"insert",tableIndex:0,key:"7",values:{value:12}},{kind:"insert",tableIndex:0,key:"8",values:{value:3}},{kind:"scan",tableIndex:0,lower:"1",upper:"9",predicate:{field:"value",operator:"gt",value:10}}],10n,2).steps[2].result.rows.map(row=>row.key),["7"]);
const ruResult=model.execute([{name:"one",tree:invalidTree,rows:[]}],[{kind:"read",tableIndex:0,key:"9"}],8n,2,null,{mode:"ru"});
assert.equal(ruResult.steps[0].result.status,"found");
assert.equal(ruResult.steps[0].result.rows[0].status,"PHYSICALLY ACCESSED");
const rcReload=model.execute([{name:"one",tree,rows:[]}],[{kind:"scan",tableIndex:0,lower:"0",upper:"99"}],10n,2,null,{mode:"rc",glcBase:10n,reloadGlcPerTuple:true});
assert.deepEqual(rcReload.statementSnapshots,["11"]);
assert(rcReload.visibilityChecks>=1);
assert.deepEqual(rcReload.statementVisibilityChecks,[rcReload.visibilityChecks]);
assert.equal(rcReload.lastGlc,"11");
const rcWrites=model.execute([{name:"one",tree:cleanTree,rows:[]}],[
  {kind:"read",tableIndex:0,key:"1"},
  {kind:"insert",tableIndex:0,key:"1",values:{}},
  {kind:"read",tableIndex:0,key:"1"},
  {kind:"update",tableIndex:0,key:"1",values:{value:"changed"}}
],10n,2,null,{mode:"rc",glcBase:10n});
assert.deepEqual(rcWrites.statementSnapshots,["11","11","12","12"]);
assert.equal(rcWrites.lastGlc,"13");
console.log("BatStore snapshot model: legacy dump, metadata, phantoms, OSIC/RC/RU visibility, and transaction blocks passed.");
