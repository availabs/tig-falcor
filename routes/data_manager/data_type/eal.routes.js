/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  ealFromEal
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `eal[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].data`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const rows = await ealFromEal(pgEnv, sourceId, viewId);
              result.push({
                path: ['eal', pgEnv, 'source', sourceId, 'view', viewId, 'data'],
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
