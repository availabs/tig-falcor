#!/usr/bin/env node

const { mkdirSync } = require("fs");
const { join } = require("path");

const falcorGraph = require("../utils/graph");
const logDbQueries = require("../utils/logDbQueries");

const logsDir = join(__dirname, "../logs");
const logPath = join(logsDir, "pm3.versionedCalculations.versionIds.ndjson");

mkdirSync(logsDir, { recursive: true });

logDbQueries(logPath);

const getQueryParams = (paths) => ({
  queryStringParameters: {
    paths,
    method: "get",
  },
});

const run = async () => {
  const pathSet = ["pm3", "versionedCalculations", "versionIds"];
  await falcorGraph.respond(getQueryParams([pathSet]));

  falcorGraph.close();
};

run();
