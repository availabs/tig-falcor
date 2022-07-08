/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const { atom: $atom, ref: $ref } = require("falcor-json-graph");
const _ = require("lodash");

const {
  SourceAttributes,
  ViewAttributes,

  getSourcesLength,
  getSourceIdsByIndex,
  getSourceAttributes,
  setSourceAttributes,
  getSourceViewsLength,
  getViewsIdsByViewIdxBySourceId,
  getViewAttributes,
  setViewAttributes
} = require("./controller");

const sourceAttrs = SourceAttributes.map((attr) => `"${attr}"`);
const viewAttrs = ViewAttributes.map((attr) => `"${attr}"`);

module.exports = [
  {
    route: "datamanager.sources.length",
    get: async function() {
      try {
        const sourcesLength = await getSourcesLength();

        console.log('test 123', sourcesLength)
        const result = [
          {
            path: ["datamanager", "sources", "length"],
            value: sourcesLength,
          },
        ];

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
  {
    route: `datamanager.sources.byId[{keys:sourceIds}].attributes[${sourceAttrs}]`,
    get: async function(pathSet) {
      try {
        const [, , , sourceIds, , attributes] = pathSet;

        const d = await getSourceAttributes(sourceIds, attributes);

        return sourceIds.reduce((acc, id) => {
          for (const attr of attributes) {
            const v = _.get(d, [id, attr], null);

            acc.push({
              path: ["datamanager", "sources", "byId", id, "attributes", attr],
              value: typeof v === "object" ? $atom(v) : v,
            });
          }

          return acc;
        }, []);
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
    set: function (jsonGraph) {
      const sourcesById = jsonGraph.datamanager.sources.byId
      const ids = Object.keys(sourcesById);
      //console.log('jsonGraph', JSON.stringify(jsonGraph))
      return setSourceAttributes(sourcesById)
        .then(rows =>{
            const result = [];
            ids.forEach(id => {
                const row = rows.reduce((a, c) => +c.id === +id ? c : a, null);
                if (!row) {
                    result.push({
                        path: ["datamanager", "sources", "byId",id],
                        value: $atom(null)
                    })
                }
                else {
                    for (const key in row) {
                        result.push({
                            path: ["datamanager", "sources", "byId",id,'attributes',key],
                            value: typeof row[key] === 'object' ? $atom(row[key]) : row[key]
                        })
                    }
                }
            });
            return result;
        })
    }
  },

  {
    route: `datamanager.sources.byId[{keys:sourceIds}].views.length`,
    get: async function(pathSet) {
      try {
        const [, , , sourceIds] = pathSet;

        const viewsLengthBySourceId = await getSourceViewsLength(sourceIds);

        return sourceIds.reduce((acc, srcId) => {
          const viewsLength = viewsLengthBySourceId[srcId] || 0;

          acc.push({
            path: ["datamanager", "sources", "byId", srcId, "views", "length"],
            value: viewsLength,
          });

          return acc;
        }, []);
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },

  {
    route: `datamanager.sources.byId[{keys:sourceIds}].views.byIndex[{integers:viewIndexes}]`,
    get: async function(pathSet) {
      try {
        const [, , , sourceIds, , , viewIndexes] = pathSet;

        const viewsIdsByViewIdxBySourceId = await getViewsIdsByViewIdxBySourceId(
          sourceIds,
          viewIndexes
        );

        const response = [];

        for (const srcId of sourceIds) {
          for (const viewIdx of viewIndexes) {
            const viewId = _.get(
              viewsIdsByViewIdxBySourceId,
              [srcId, viewIdx],
              null
            );

            response.push({
              path: [
                "datamanager",
                "sources",
                "byId",
                srcId,
                "views",
                "byIndex",
                viewIdx,
              ],
              value:
                viewId !== null
                  ? $ref(["datamanager", "views", "byId", viewId])
                  : null,
            });
          }
        }

        return response;
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  },

  {
    route: `datamanager.sources.byIndex[{integers:indices}]`,
    get: async function(pathSet) {
      //console.log(pathSet);
      try {
        const sourceIndexes = pathSet.indices.map((idx) => +idx);

        const idsByIdx = await getSourceIdsByIndex(sourceIndexes);

        return sourceIndexes.reduce((acc, idx) => {
          const id = idsByIdx[idx];

          acc.push({
            path: ["datamanager", "sources", "byIndex", idx],
            value: $ref(["datamanager", "sources", "byId", id]),
          });

          return acc;
        }, []);
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },

  {
    route: `datamanager.views.byId[{keys:ids}].attributes[${viewAttrs}]`,
    get: async function(pathSet) {
      try {
        const [, , , ids, , attributes] = pathSet;

        const d = await getViewAttributes(ids, attributes);

        return ids.reduce((acc, id) => {
          attributes.forEach((attr) => {
            const v = _.get(d, [id, attr], null);

            acc.push({
              path: ["datamanager", "views", "byId", id, "attributes", attr],
              value: typeof v === "object" ? $atom(v) : v, //$atom(value),
            });
          });

          return acc;
        }, []);
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
    set: function (jsonGraph) {
      const viewById = jsonGraph.datamanager.views.byId
      const ids = Object.keys(viewById);
      // console.log('jsonGraph', JSON.stringify(jsonGraph))
      return setViewAttributes(viewById)
        .then(rows =>{
            // console.log('rows', rows)
            const result = [];
            ids.forEach(id => {
                const row = rows.reduce((a, c) => +c.id === +id ? c : a, null);
                if (!row) {
                    result.push({
                        path: ["datamanager", "views", "byId",id],
                        value: $atom(null)
                    })
                }
                else {
                    for (const key in row) {
                        result.push({
                            path: ["datamanager", "views", "byId",id,'attributes',key],
                            value: typeof row[key] === 'object' ? $atom(row[key]) : row[key]
                        })
                    }
                }
            });
            return result;
        })
    }
    
  },
];
