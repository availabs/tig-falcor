/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  listPgEnvs,

  getSourcesLength,
  getSourceIdsByIndex,
  getSourceAttributes,
  setSourceAttributes,
  getSourceViewsLength,
  getViewsIdsByViewIdxBySourceId,
  getViewAttributes,
  setViewAttributes,

  queryViewTabledataLength,
  queryViewTabledataIndices,
  queryViewTabledataById,

  getViewDependencySubgraph,

  getEtlContext,
  getEtlContextsLatestEventByDamaSourceId,
  getDependentsSafetyCheck,
  getSourceMetaData,

  getSourceIdsByName,
} = require("./controller/multi-db");

const { getLtsView } = require("./controller/data_type_controller");

const SourceAttributes = require("./constants/SourceAttributes");
const ViewAttributes = require("./constants/ViewAttributes");

const sourceAttrs = SourceAttributes.map((attr) => `"${attr}"`);
const viewAttrs = ViewAttributes.map((attr) => `"${attr}"`);

module.exports = [
  // -----------------------------------------------
  // --- General Dama Settings Section 
  // -----------------------------------------------
  {
    route: "dama-info.pgEnvs",
    get: async function() {
      try {
        const pgEnvs = await listPgEnvs();
        console.log('dama-info.pgEnvs', pgEnvs)
        return [
          {
            path: ["dama-info", "pgEnvs"],
            value: $atom(pgEnvs),
          },
        ];
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },

  {
    route: "dama-info.sourceAttributes",
    get: async function() {
      return {
        path: ["dama-info", "sourceAttributes"],
        value: $atom(SourceAttributes),
      };
    },
  },

  {
    route: "dama-info.viewAttributes",
    get: async function() {
      return {
        path: ["dama-info", "viewAttributes"],
        value: $atom(ViewAttributes),
      };
    },
  },
  // --- END General Dama Settings Section 
  

  // -----------------------------------------------
  // --- Dama Source Routes
  // -----------------------------------------------

  {
    route: "dama[{keys:pgEnvs}].sources.length",
    get: async function(pathSet) {
      try {
        const [, pgEnvs] = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const sourcesLength = await getSourcesLength(pgEnv);

          result.push({
            path: ["dama", pgEnv, "sources", "length"],
            value: sourcesLength,
          });
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },

  {
    route: `dama[{keys:pgEnvs}].sources.byIndex[{integers:indices}]`,
    get: async function(pathSet) {
      //console.log(pathSet);
      try {
        const [, pgEnvs] = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const sourceIndexes = pathSet.indices.map((idx) => +idx);

          const idsByIdx = await getSourceIdsByIndex(pgEnv, sourceIndexes);

          for (const srcIdx of sourceIndexes) {
            const id = idsByIdx[srcIdx];

            result.push({
              path: ["dama", pgEnv, "sources", "byIndex", srcIdx],
              value: $ref(["dama", pgEnv, "sources", "byId", id]),
            });
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
    route: `dama[{keys:pgEnvs}].sources.byId[{keys:sourceIds}].attributes[${sourceAttrs}]`,
    get: async function(pathSet) {
      try {
        const [, pgEnvs, , , sourceIds, , attributes] = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const d = await getSourceAttributes(pgEnv, sourceIds, attributes);

          for (const sourceId of sourceIds) {
            for (const attr of attributes) {
              const v = _.get(d, [sourceId, attr], null);

              result.push({
                path: [
                  "dama",
                  pgEnv,
                  "sources",
                  "byId",
                  sourceId,
                  "attributes",
                  attr,
                ],
                value: typeof v === "object" ? $atom(v) : v,
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

    set: async function(jsonGraph) {
      try {
        const pgEnvs = Object.keys(jsonGraph.dama);

        const result = [];

        for (const pgEnv of pgEnvs) {
          const sourcesById = jsonGraph.dama[pgEnv].sources.byId;

          const ids = Object.keys(sourcesById);

          const rows = await setSourceAttributes(pgEnv, sourcesById);

          for (const id of ids) {
            const row = rows.reduce(
              (a, c) => (+c.source_id === +id ? c : a),
              null
            );

            console.log("testing", rows);
            if (!row) {
              result.push({
                path: ["dama", pgEnv, "sources", "byId", id],
                value: $atom(null),
              });
            } else {
              console.log("set attribute returning a value", row);
              for (const key in row) {
                result.push({
                  path: [
                    "dama",
                    pgEnv,
                    "sources",
                    "byId",
                    id,
                    "attributes",
                    key,
                  ],
                  value:
                    typeof row[key] === "object" ? $atom(row[key]) : row[key],
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
    route: `dama[{keys:pgEnvs}].sources.byId[{keys:sourceIds}].views.byIndex[{integers:viewIndexes}]`,
    get: async function(pathSet) {
      try {
        const [, pgEnvs, , , sourceIds, , , viewIndexes] = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const viewsIdsByViewIdxBySourceId = await getViewsIdsByViewIdxBySourceId(
            pgEnv,
            sourceIds,
            viewIndexes
          );

          for (const srcId of sourceIds) {
            for (const viewIdx of viewIndexes) {
              const viewId = _.get(
                viewsIdsByViewIdxBySourceId,
                [srcId, viewIdx],
                null
              );

              result.push({
                path: [
                  "dama",
                  pgEnv,
                  "sources",
                  "byId",
                  srcId,
                  "views",
                  "byIndex",
                  viewIdx,
                ],
                value:
                  viewId !== null
                    ? $ref(["dama", pgEnv, "views", "byId", viewId])
                    : null,
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
    route: "dama[{keys:pgEnvs}].sources.byId[{keys:sourceIds}].dependents",
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const sourceId of sourceIds) {
            const dependents = await getDependentsSafetyCheck(
              pgEnv,
              sourceId,
              "source_id"
            );

            result.push({
              path: ["dama", pgEnv, "sources", "byId", sourceId, "dependents"],
              value: $atom(dependents),
            });
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
  // --- END Dama Source Routes
  
  // -----------------------------------------------
  // --- Dama Views Routes
  // -----------------------------------------------
  {
    route: `dama[{keys:pgEnvs}].sources.byId[{keys:sourceIds}].views.length`,
    get: async function(pathSet) {
      try {
        const [, pgEnvs, , , sourceIds] = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const viewsLengthBySourceId = await getSourceViewsLength(
            pgEnv,
            sourceIds
          );

          for (const sourceId of sourceIds) {
            const viewsLength = viewsLengthBySourceId[sourceId] || 0;

            result.push({
              path: [
                "dama",
                pgEnv,
                "sources",
                "byId",
                sourceId,
                "views",
                "length",
              ],
              value: viewsLength,
            });
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
    route: `dama[{keys:pgEnvs}].views.byId[{keys:ids}].attributes[${viewAttrs}]`,
    get: async function(pathSet) {
      try {
        const [, pgEnvs, , , ids, , attributes] = pathSet;
        console.log('get view attributes', ids, attributes)
      
        const result = [];

        for (const pgEnv of pgEnvs) {
          const d = await getViewAttributes(pgEnv, ids, attributes);

          for (const id of ids) {
            attributes.forEach((attr) => {
              const v = _.get(d, [id, attr], null);

              result.push({
                path: ["dama", pgEnv, "views", "byId", id, "attributes", attr],
                value: typeof v === "object" ? $atom(v) : v, //$atom(value),
              });
            });
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
    set: async function(jsonGraph) {
      try {
        const pgEnvs = Object.keys(jsonGraph.dama);

        const result = [];

        for (const pgEnv of pgEnvs) {
          const viewById = jsonGraph.dama[pgEnv].views.byId;

          const ids = Object.keys(viewById);

          const rows = await setViewAttributes(pgEnv, viewById);

          ids.forEach((id) => {
            const row = rows.reduce(
              (a, c) => (+c.view_id === +id ? c : a),
              null
            );
            if (!row) {
              result.push({
                path: ["dama", pgEnv, "views", "byId", id],
                value: $atom(null),
              });
            } else {
              for (const key in row) {
                result.push({
                  path: ["dama", pgEnv, "views", "byId", id, "attributes", key],
                  value:
                    typeof row[key] === "object" ? $atom(row[key]) : row[key],
                });
              }
            }
          });
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },

  {
    route: "dama[{keys:pgEnvs}].views.byId[{keys:viewIds}].dependents",
    get: async function(pathSet) {
      try {
        const { pgEnvs, viewIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const viewId of viewIds) {
            const dependents = await getDependentsSafetyCheck(pgEnv, viewId);

            result.push({
              path: ["dama", pgEnv, "views", "byId", viewId, "dependents"],
              value: $atom(dependents),
            });
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
    route: `dama[{keys:pgEnvs}].viewDependencySubgraphs.byViewId[{keys:viewIds}]`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, viewIds } = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const id of viewIds) {
            const viewDependencySubgraph = await getViewDependencySubgraph(
              pgEnv,
              id
            );

            result.push({
              path: ["dama", pgEnv, "viewDependencySubgraphs", "byViewId", id],
              value: $atom(viewDependencySubgraph),
            });
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
    route: `dama[{keys:pgEnvs}].sources.byId[{keys:sourceIds}].views.lts`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, sourceIds } = pathSet;
        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const sourceId of sourceIds) {
            const rows = await getLtsView(pgEnv, sourceId);
            result.push({
              path: [
                "dama",
                pgEnv,
                "sources",
                "byId",
                sourceId,
                "views",
                "lts",
              ],
              value: $atom(
                _.get(
                  rows.find((r) => r.source_id === sourceId),
                  "view_id"
                )
              ),
            });
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
  // --- END Dama Views Routes


  // -----------------------------------------------
  // --- Dama Views Table Data Routes
  // -----------------------------------------------
  {
    route: `dama[{keys:pgEnvs}].viewsbyId[{keys:damaViewIds}].data.length`,
    get: async function(pathSet) {
      console.log('getting view data length')
      try {
        const { pgEnvs, damaViewIds  } = pathSet;

        //console.log('getting view data length', pgEnvs, damaViewIds)
        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const viewId of damaViewIds) {
            console.time('get ViewData Length')
            const numRows = await queryViewTabledataLength(
              pgEnv,
              viewId
            );
            console.timeEnd('get ViewData Length')

            // console.log('numRows', numRows)

            result.push({
              path: ["dama", pgEnv, "viewsbyId", viewId, "data", "length"],
              value: +numRows, //$atom(value),
            });
              
            
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
    route: `dama[{keys:pgEnvs}].viewsbyId[{keys:damaViewIds}].databyIndex[{integers:dataIndices}]`,
    get: async function(pathSet) {
      console.log('getting view data length')
      try {
        const { pgEnvs, damaViewIds, dataIndices } = pathSet;

        //console.log('getting view data length', pgEnvs, damaViewIds)
        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const viewId of damaViewIds) {
            console.time('get ViewData Indices')
            const rows = await queryViewTabledataIndices(
              pgEnv,
              viewId
            );
            console.timeEnd('get ViewData Indices')

            //console.log('idRows', rows)
            dataIndices.forEach((di,i) => {
              if(rows?.[di]?.id || rows?.[di]?.id === 0) {
                result.push({
                  path: ["dama", pgEnv, "viewsbyId", viewId, "databyIndex", di],
                  value: $ref(["dama", pgEnv, "viewsbyId", viewId, 'databyId', rows[di].id]), //$atom(value),
                });
              }
            })
              
            
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
    route: `dama[{keys:pgEnvs}].viewsbyId[{keys:damaViewIds}].databyId[{keys:intIds}][{keys:attributes}]`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, damaViewIds, intIds, attributes } = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const viewId of damaViewIds) {
            console.time('get ViewData Data')
            const d = await queryViewTabledataById(
              pgEnv,
              viewId,
              intIds,
              attributes
            );
            console.timeEnd('get ViewData Data')

            for (const id of intIds) {
              attributes.forEach((attr) => {
                const v = _.get(d, [id, attr], null);

                result.push({
                  path: ["dama", pgEnv, "viewsbyId", viewId, "databyId", id, attr],
                  value:  v, //$atom(value),
                });
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
  // --- END Dama Views Table Data Routes
  
  // -----------------------------------------------
  // --- Dama ETL Context  Routes
  // -----------------------------------------------
  
  {
    route: `dama[{keys:pgEnvs}].etlContexts.byEtlContextId[{keys:etlContextIds}]`,
    get: async function(pathSet) {
      try {
        const { pgEnvs, etlContextIds } = pathSet;

        console.log(JSON.stringify({ pgEnvs, etlContextIds }, null, 4));

        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const etlContextId of etlContextIds) {
            const ctx = await getEtlContext(pgEnv, etlContextId);

            result.push({
              path: [
                "dama",
                pgEnv,
                "etlContexts",
                "byEtlContextId",
                etlContextId,
              ],
              value: $atom(ctx),
            });
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
    route: `dama[{keys:pgEnvs}].etlContexts.byDamaSourceId[{keys:damaSourceIds}]['RUNNING', 'STOPPED']`,
    get: async function(pathSet) {
      try {
        const [, , , , , etlCtxStatuses] = pathSet;

        const { pgEnvs } = pathSet;

        let { damaSourceIds } = pathSet;
        damaSourceIds = _.uniq(damaSourceIds).map((id) => +id);

        const result = [];

        for (const pgEnv of pgEnvs) {
          for (const ctxStatus of etlCtxStatuses) {
            // NOTE: OPEN, DONE, and ERROR partition the events.
            const etlCtxStates =
              ctxStatus === "RUNNING" ? ["OPEN"] : ["DONE", "ERROR"];

            const seenDamaSrcIds = new Set();

            const etlContextsByDamaSourceId = await getEtlContextsLatestEventByDamaSourceId(
              pgEnv,
              damaSourceIds,
              etlCtxStates
            );

            const ids = Object.keys(etlContextsByDamaSourceId).map((id) => +id);

            for (const id of ids) {
              seenDamaSrcIds.add(id);

              const etlContexts = etlContextsByDamaSourceId[id] || {};

              result.push({
                path: [
                  "dama",
                  pgEnv,
                  "etlContexts",
                  "byDamaSourceId",
                  id,
                  ctxStatus,
                ],
                value: $atom(etlContexts),
              });
            }

            for (const id of damaSourceIds) {
              if (!seenDamaSrcIds.has(id)) {
                seenDamaSrcIds.add(id);

                result.push({
                  path: [
                    "dama",
                    pgEnv,
                    "etlContexts",
                    "byDamaSourceId",
                    id,
                    ctxStatus,
                  ],
                  value: $atom(null),
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
  }
];
