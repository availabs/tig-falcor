#!/usr/bin/env node

const { readFileSync, writeFileSync } = require("fs");
const { join } = require("path");

const { npmrds_db: db } = require("../../../db_service");

const fillQueryParameters = require("../utils/fillQueryParameters");

const logPath = join(__dirname, "../logs", "geoAttributes.ndjson");
const queryPlanPath = join(
  __dirname,
  "../logs",
  "geoAttributes.queryPlan.json"
);

const query = JSON.parse(readFileSync(logPath, { encoding: "utf8" }));

const text = `EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)\n${query.text.trim()}`;
const { values } = query;

console.log(fillQueryParameters(query.text, query.values));

// async function run() {
// const {
// rows: [{ ["QUERY PLAN"]: queryPlan }],
// } = await db.query(text, values);

// writeFileSync(queryPlanPath, JSON.stringify(queryPlan));

// db.end();
// }

// run();
