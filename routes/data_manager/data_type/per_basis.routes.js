/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  per_basis_stats
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `per_basis[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].stats`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          const rows = await per_basis_stats(pgEnv, sourceIds, viewIds);

          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              result.push({
                path: ['per_basis', pgEnv, 'source', sourceId, 'view', viewId, 'stats'],
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
