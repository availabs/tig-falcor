/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  nriTotals,
  simpleNRISelect
} = require("../controller/data_type_controller");

module.exports = [
  {
    route: `nri[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].totals`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const rows = await nriTotals(pgEnv, sourceId, viewId);
              result.push({
                path: ['nri', pgEnv, 'source', sourceId, 'view', viewId, 'totals'],
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
  {
    route: `nri[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].byGeoid[{keys:geoids}][{keys:attributes}]`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds, viewIds, geoids, attributes } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for(const sourceId of sourceIds){
            for (const viewId of viewIds) {
              const rows = await simpleNRISelect(pgEnv, sourceId, viewId, 'stcofips', geoids, attributes);

              for (const geoid of geoids) {
                const res = _.get(rows.find(r => r.source_id === sourceId && r.view_id === viewId), 'rows', []).find(row => row.id === geoid.toString())

                for( const attribute of attributes ){
                  result.push({
                    path: ['nri', pgEnv, 'source', sourceId, 'view', viewId, 'byGeoid', geoid, attribute],
                    value: $atom((res || {})[attribute])
                  });
                }
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
];
