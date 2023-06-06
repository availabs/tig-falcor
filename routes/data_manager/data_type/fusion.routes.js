/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  fusionLossByYearByDisasterNumberTotal,
  fusionLossByYearByDisasterNumberByGeoid,
  fusionValidateLosses,
  fusionDataSourcesBreakdown
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `fusion[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].total.lossByYearByDisasterNumber`,
    get: async function(pathSet) {
      try {
        // geoids can be 'all'
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await fusionLossByYearByDisasterNumberTotal(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const res = _.get(rows.find(r => r.source_id === sourceId && r.view_id === viewId), 'rows');
              result.push({
                path: ['fusion', pgEnv, 'source', sourceId, 'view', viewId, 'total', 'lossByYearByDisasterNumber'],
                value: $atom(res)
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
    route: `fusion[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].byGeoid[{keys:geoids}].lossByYearByDisasterNumber`,
    get: async function(pathSet) {
      try {
        // geoids can be 'all'
        const { pgEnvs, sourceIds, viewIds, geoids } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await fusionLossByYearByDisasterNumberByGeoid(pgEnv, sourceIds, viewIds, geoids);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const res = _.get(rows.find(r => r.source_id === sourceId && r.view_id === viewId), 'rows', []);
              for(const geoid of geoids) {
                result.push({
                  path: ['fusion', pgEnv, 'source', sourceId, 'view', viewId, 'byGeoid', geoid, 'lossByYearByDisasterNumber'],
                  value: $atom(res?.filter(r => r.geoid.toString() === geoid.toString()))
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
    route: `fusion[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].validateLosses`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await fusionValidateLosses(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['fusion', pgEnv, 'source', sourceId, 'view', viewId, 'validateLosses'],
                value: $atom(
                  _.get(rows.find(
                    r => r.source_id === sourceId && r.view_id === viewId
                  ), 'res')
                )
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
    route: `fusion[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].dataSourcesBreakdown`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await fusionDataSourcesBreakdown(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['fusion', pgEnv, 'source', sourceId, 'view', viewId, 'dataSourcesBreakdown'],
                value: $atom(
                  _.get(rows.find(
                    r => r.source_id === sourceId && r.view_id === viewId
                  ), 'rows')
                )
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
