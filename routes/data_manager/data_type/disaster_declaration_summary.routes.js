const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");


const {
    ddCount,
    incidentByMonth
  } = require("../controller/data_type_controller");

  module.exports = [
    {
      route: `disaster_declaration_summary[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].count`,
      get: async function(pathSet) {
        try {
          const { pgEnvs, sourceIds, viewIds } = pathSet;
          const result = [];

          console.log(sourceIds);
          console.log(pgEnvs);
          console.log(viewIds);

          for (const pgEnv of pgEnvs) {
            const rows = await ddCount(pgEnv, sourceIds, viewIds);
            console.log('rows', rows);
            for(const sourceId of sourceIds){
              for (const viewId of viewIds) {
                result.push({
                  path: ['disaster_declaration_summary', pgEnv, 'source', sourceId, 'view', viewId, 'count'],
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
      route: `disaster_declaration_summary[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].countbymonth`,
      get: async function(pathSet) {
        try {
          const { pgEnvs, sourceIds, viewIds } = pathSet;
          const result = [];

          console.log(sourceIds);
          console.log(pgEnvs);
          console.log(viewIds);

          for (const pgEnv of pgEnvs) {
            const rows = await incidentByMonth(pgEnv, sourceIds, viewIds);
            console.log('rows', rows);
            for(const sourceId of sourceIds){
              for (const viewId of viewIds) {
                result.push({
                  path: ['disaster_declaration_summary', pgEnv, 'source', sourceId, 'view', viewId, 'countbymonth'],
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
    }
  ];