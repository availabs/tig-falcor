#!/usr/bin/env node

const { mkdirSync } = require("fs");
const { join } = require("path");

const falcorGraph = require("../utils/graph");
const logDbQueries = require("../utils/logDbQueries");

const logsDir = join(__dirname, "../logs");
const logPath = join(logsDir, "severeWeatherAllTime.old.ndjson");

mkdirSync(logsDir, { recursive: true });

logDbQueries(logPath);

const getQueryParams = (paths) => ({
  queryStringParameters: {
    paths,
    method: "get",
  },
});

const pathSet = [
  "severeWeather",
  36,
  "earthquake",
  "allTime",
  [
    "annualized_damage",
    "annualized_num_events",
    "annualized_num_severe_events",
    "daily_event_prob",
    "daily_severe_event_prob",
    "fatalities",
    "injuries",
  ],
];

const run = async () => {
  try {
    const result = await falcorGraph.respond(getQueryParams([pathSet]));
    console.log(JSON.stringify(result, null, 4));
  } catch (err) {
    console.error(err);
  }

  falcorGraph.close();
};

run();
