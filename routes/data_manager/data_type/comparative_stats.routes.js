/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  comparativeStats,
  comparativeStatsMega,
  EALByGeoidByNriCat
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `comparative_stats[{keys:pgEnvs}].byEalIds.source[{keys:sourceIds}].view[{keys:viewIds}]`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const rows = await comparativeStats(pgEnv, sourceId, viewId);
              result.push({
                path: ['comparative_stats', pgEnv, 'byEalIds', 'source', sourceId, 'view', viewId],
                value: $atom(rows)
              });
            }
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
  {
    route: `comparative_stats[{keys:pgEnvs}].byEalIds.source[{keys:sourceIds}].view[{keys:viewIds}].byGeoid[{keys:geoids}]`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds, geoids } = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const rows = await EALByGeoidByNriCat(pgEnv, sourceId, viewId, geoids);

              for(const geoid of geoids) {
                result.push({
                  path: ['comparative_stats', pgEnv, 'byEalIds', 'source', sourceId, 'view', viewId, 'byGeoid', geoid],
                  value: $atom(rows.filter(r => geoid === 'all' || r.geoid.toString() === geoid.toString()))
                });
              }
            }
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
  {
    route: `comparative_stats[{keys:pgEnvs}].byEalIds.source[{keys:sourceIds}].view[{keys:viewIds}].mega`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const rows = await comparativeStatsMega(pgEnv, sourceId, viewId);
              result.push({
                path: ['comparative_stats', pgEnv, 'byEalIds', 'source', sourceId, 'view', viewId, 'mega'],
                value: $atom(rows)
              });
            }
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
];
