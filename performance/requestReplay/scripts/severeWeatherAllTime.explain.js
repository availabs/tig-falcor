#!/usr/bin/env node

const { readFileSync } = require("fs");
const { join } = require("path");

const { hazmit_db } = require("../../../db_service");

const logPath = join(__dirname, "../logs", "severeWeatherAllTime.old.ndjson");

const query = JSON.parse(readFileSync(logPath, { encoding: "utf8" }));

const text = `EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)\n${query.text.trim()}`;

async function run() {
  const {
    rows: [{ ["QUERY PLAN"]: queryPlan }],
  } = await hazmit_db.query(text);

  console.log(JSON.stringify(queryPlan));

  hazmit_db.end();
}

run();
