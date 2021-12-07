#!/usr/bin/env node

const { mkdirSync } = require("fs");
const { join } = require("path");

const falcorGraph = require("../utils/graph");
const logDbQueries = require("../utils/logDbQueries");

const logsDir = join(__dirname, "../logs");
const logPath = join(logsDir, "acsCensusAcsByGeoidByYearByKeydb.ndjson");

mkdirSync(logsDir, { recursive: true });

logDbQueries(logPath);

const getQueryParams = (paths) => ({
  queryStringParameters: {
    paths,
    method: "get",
  },
});

const pathSet = [
  "acs",
  [
    36093020101,
    36093020102,
    36093020200,
    36093020300,
    36093020500,
    36093020600,
    36093020700,
    36093020800,
    36093020900,
    36093021001,
    36093021002,
    36093021200,
    36093021400,
    36093021500,
    36093021600,
    36093021700,
    36093021801,
    36093021802,
    36093031901,
    36093031902,
    36093032000,
    36093032101,
    36093032102,
    36093032200,
    36093032300,
    36093032402,
    36093032403,
    36093032404,
    36093032502,
    36093032503,
    36093032504,
    36093032601,
    36093032602,
    36093032700,
    36093032901,
    36093032902,
    36093033002,
    36093033003,
    36093033004,
    36093033101,
    36093033102,
    36093033200,
    36093033300,
    36093033400,
    36093033500,
  ],
  2019,
  [
    "S2501_C01_002E",
    "S2501_C01_003E",
    "S2501_C01_004E",
    "S2501_C01_005E",
    "B08202_002E",
    "B08202_003E",
    "B08202_004E",
    "B08202_005E",
    "S0101_C01_002E",
    "S0101_C01_003E",
    "S0101_C01_004E",
    "S0101_C01_005E",
    "S0101_C01_006E",
    "S0101_C01_007E",
    "S0101_C01_008E",
    "S0101_C01_009E",
    "S0101_C01_010E",
    "S0101_C01_011E",
    "S0101_C01_012E",
    "S0101_C01_013E",
    "S0101_C01_014E",
    "S0101_C01_015E",
    "S0101_C01_016E",
    "S0101_C01_017E",
    "S0101_C01_018E",
    "S0101_C01_019E",
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
