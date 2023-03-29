/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
    numRows,
    eventsByYear,
    eventsByType
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `ncei_storm_events[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].numRows`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await numRows(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['ncei_storm_events', pgEnv, 'source', sourceId, 'view', viewId, 'numRows'],
                value: $atom(
                  _.get(rows.find(
                    r => r.source_id === sourceId && r.view_id === viewId
                  ), 'num_rows')
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
    route: `ncei_storm_events[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].eventsByYear`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await eventsByYear(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['ncei_storm_events', pgEnv, 'source', sourceId, 'view', viewId, 'eventsByYear'],
                value: $atom(
                  _.get(rows.find(
                    r => r.source_id === sourceId && r.view_id === viewId
                  ), 'eventsByYear')
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
    route: `ncei_storm_events[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].eventsByType`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await eventsByType(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['ncei_storm_events', pgEnv, 'source', sourceId, 'view', viewId, 'eventsByType'],
                value: $atom(
                  _.get(rows.find(
                    r => r.source_id === sourceId && r.view_id === viewId
                  ), 'eventsByType')
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
