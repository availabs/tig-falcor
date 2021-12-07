const falcorGraph = require("./graph");

const geographies = ["UA|63217"]; //["COUNTY|36001", "COUNTY|36091"];
const networks = ["tmc"];
const yearMonths = ["01|2019"];

describe("TIG NPMRDS Test", () => {
  test("Get get ", (done) => {
    const query = {
      paths: [["tig", "npmrds", yearMonths, geographies, "data"]],
      method: "get",
    };

    //console.log(JSON.stringify(query, null, 3));
    falcorGraph.respond(
      { queryStringParameters: query },
      (error, { jsonGraph }) => {
        console.log("got data", JSON.stringify(jsonGraph, null, 3));
        done();
      }
    );
  }, 100000);
});

afterAll(() => {
  return falcorGraph.close();
});
