var { get, map } = require("lodash");

const falcorGraph = require("./test/graph");
const { listPgEnvs, getDb } = require("./routes/data_manager/databases");
const AcsService = require("./services/acsController");

const pgEnv = "pan";

const chunkSize = 1;
const geoids = [34, 36, "09"];
const years = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019];
const censusKeys = [
  "B02001_001E",
  "B02001_004E",
  "B02001_005E",
  "B02001_003E",
  "B02001_006E",
  "B02001_007E",
  "B02001_008E",
  "B02001_002E",
];

const chunkArray = (array, size) => {
  const chunkedArray = [];
  for (let i = 0; i < array.length; i += size) {
    const chunk = array.slice(i, i + size);
    chunkedArray.push(chunk);
  }
  return chunkedArray;
};

const geoidsChunks = chunkArray(geoids, chunkSize);
const processChunks = async (geoidsChunks, years, censusKeys) => {
  let currentChunk = 0;

  // for (let geoids of geoidsChunks) {
  console.log(
    `\n\n\n  ------------- Started Chunk: ${currentChunk}: ---------- \n\n\n`
  );
  // const getCountiesEvent = {
  //   paths: [["geo", geoids.map(String), "counties"]],
  //   method: "get"
  // };
  // const getTractsEvent = {
  //   paths: [["geo", geoids.map(String), "tracts"]],
  //   method: "get"
  // };

  // const t = await falcorGraph.respond({
  //   queryStringParameters: getCountiesEvent
  // });
  // const v = await falcorGraph.respond({
  //   queryStringParameters: getTractsEvent
  // });
  const db = await getDb(pgEnv);
  let t = await db.query(
    "select geoid from geo.tl_2017_county_1078 where statefp = ANY(ARRAY['34', '36', '09']::int[]);"
  );
  let v = await db.query(
    "select geoid from geo.tl_2017_tract_990 where statefp = ANY(ARRAY['34', '36', '09']::int[]);"
  );

  if (t && t.rows) {
    t = map(get(t, "rows"), "geoid");
  }

  if (v && v.rows) {
    v = map(get(v, "rows"), "geoid");
  }

  // let geo = [
  //   ...(geoids || []).reduce((a, c) => {
  //     a = [
  //       ...a,
  //       ...get(t, ["jsonGraph", "geo", c, "counties", "value"], []),
  //       ...get(v, ["jsonGraph", "geo", c, "tracts", "value"], [])
  //     ];
  //     return a;
  //   }, new Set())
  // ];

  let geo = new Set([...t, ...v]);
  console.log("geo", geo);
  // console.log("geo", geo, geoids);
  const getEvent = {
    paths: [["acs", geo, years, censusKeys]],
    method: "get",
  };

  await AcsService.CensusAcsByGeoidByYearByKey(geo, years, censusKeys);
  // await falcorGraph.respond({ queryStringParameters: getEvent });
  console.log(
    `\n\n\n  ------------- Ended Chunk: ${currentChunk}: ---------- \n\n\n`
  );
  currentChunk++;
};
// };

processChunks(geoidsChunks, years, censusKeys)
  .then(() => {
    console.time("Completed");
  })
  .catch((err) => {
    console.error("Error occurred during processing:", err);
  });
