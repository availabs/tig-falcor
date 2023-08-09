const TigerService = require("../controller/data_types/tiger_controller");
const falcorJsonGraph = require("falcor-json-graph");
const $atom = falcorJsonGraph.atom;

const GL_MAP = TigerService.GEO_LEVEL_MAP;

const { get } = require("lodash");

module.exports = [
  {
    route:
      "dama[{keys:pgEnvs}].tiger[{keys:viewIds}][{keys:geoids}][{keys:years}][{keys:geolevels}]",
    get: async function(pathSet) {
      const [, pgEnvs, , viewIds, geoids, years, geolevels] = pathSet;
      const response = [];
      for (var pgEnv of pgEnvs) {
        const hashedResponse = await TigerService.geoLevelContains(
          pgEnv,
          viewIds,
          geoids,
          years,
          geolevels
        );

        viewIds.forEach(viewId => {
          geoids.forEach(geoid => {
            years.forEach(year => {
              geolevels.forEach(geolevel => {
                const tempGeolevel = get(GL_MAP, geolevel, geolevel);
                const forGeoLevel =
                  hashedResponse[viewId][geoid][year][tempGeolevel];
                response.push({
                  path: ["dama", pgEnv, "tiger", viewId, geoid, year, geolevel],
                  value: $atom(forGeoLevel.map(r => r.geoid))
                });
              });
            });
          });
        });
      }
      return response;
    }
  }
];
