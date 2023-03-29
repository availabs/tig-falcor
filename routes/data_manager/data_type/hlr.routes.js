/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  ealFromHlr
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `hlr[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].eal`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await ealFromHlr(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['hlr', pgEnv, 'source', sourceId, 'view', viewId, 'eal'],
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
