#!/usr/bin/env node

const { mkdirSync } = require("fs");
const { join } = require("path");

const falcorGraph = require("../utils/graph");
const logDbQueries = require("../utils/logDbQueries");

const logsDir = join(__dirname, "../logs");
const logPath = join(logsDir, "geoAttributes.ndjson");

mkdirSync(logsDir, { recursive: true });

logDbQueries(logPath);

const getQueryParams = (paths) => ({
  queryStringParameters: {
    paths,
    method: "get",
  },
});

const pathSet = [
  "geoAttributes",
  [
    "STATE|36",
    "COUNTY|36005",
    "COUNTY|36091",
    "COUNTY|36033",
    "COUNTY|36025",
    "COUNTY|36059",
    "COUNTY|36007",
    "COUNTY|36045",
    "COUNTY|36051",
    "COUNTY|36121",
    "COUNTY|36105",
    "COUNTY|36065",
    "COUNTY|36001",
    "COUNTY|36069",
    "COUNTY|36087",
    "COUNTY|36101",
    "COUNTY|36123",
    "COUNTY|36011",
    "COUNTY|36027",
    "COUNTY|36095",
    "COUNTY|36023",
    "COUNTY|36107",
    "COUNTY|36097",
    "COUNTY|36085",
    "COUNTY|36081",
    "COUNTY|36119",
    "COUNTY|36009",
    "COUNTY|36047",
    "COUNTY|36075",
    "COUNTY|36083",
    "COUNTY|36079",
    "COUNTY|36057",
    "COUNTY|36037",
    "COUNTY|36053",
    "COUNTY|36019",
    "COUNTY|36029",
    "COUNTY|36073",
    "COUNTY|36013",
    "COUNTY|36055",
    "COUNTY|36113",
    "COUNTY|36049",
    "COUNTY|36021",
    "COUNTY|36031",
    "COUNTY|36041",
    "COUNTY|36061",
    "COUNTY|36111",
    "COUNTY|36103",
    "COUNTY|36017",
    "COUNTY|36077",
    "COUNTY|36109",
    "COUNTY|36035",
    "COUNTY|36089",
    "COUNTY|36099",
    "COUNTY|36043",
    "COUNTY|36039",
    "COUNTY|36063",
    "COUNTY|36067",
    "COUNTY|36015",
    "COUNTY|36093",
    "COUNTY|36117",
    "COUNTY|36071",
    "COUNTY|36115",
    "COUNTY|36003",
    "MPO|09197504",
    "MPO|09198100",
    "MPO|34198200",
    "MPO|36196500",
    "MPO|36197200",
    "MPO|36197300",
    "MPO|36197401",
    "MPO|36197402",
    "MPO|36197500",
    "MPO|36197700",
    "MPO|36198201",
    "MPO|36198202",
    "MPO|36198203",
    "MPO|36198204",
    "MPO|36199200",
    "MPO|36200300",
    "MPO|36201400",
    "UA|00970",
    "UA|07732",
    "UA|10162",
    "UA|11350",
    "UA|27118",
    "UA|33598",
    "UA|41914",
    "UA|45262",
    "UA|63217",
    "UA|71803",
    "UA|75664",
    "UA|79633",
    "UA|86302",
    "UA|89785",
    "UA|92674",
    "UA|99998",
    "UA|99999",
    "REGION|1",
    "REGION|10",
    "REGION|11",
    "REGION|2",
    "REGION|3",
    "REGION|4",
    "REGION|5",
    "REGION|6",
    "REGION|7",
    "REGION|8",
    "REGION|9",
  ],
  { to: 2020, from: 2020 },
];

const run = async () => {
  try {
    await falcorGraph.respond(getQueryParams([pathSet]));
  } catch (err) {
    console.error(err);
  }

  falcorGraph.close();
};

run();
