const falcorJsonGraph = require("falcor-json-graph"),
    get = require('lodash.get'),
    $atom = falcorJsonGraph.atom,
    SedController = require("./sed.controller"),
    $ref = falcorJsonGraph.ref;
   

module.exports = [
     {
        route: `tig.sed_taz.bySource[{keys:source_ids}].data`,
        get: function(pathSet) {
            const source_ids = pathSet.source_ids;
            console.time(`SED TAZ Data Fetch ${source_ids.join(',')}`)
            return SedController.sedTazBySourceMem.then(fn => fn(source_ids)).then((rows) => {
                console.timeEnd(`SED TAZ Data Fetch ${source_ids.join(',')}`)
                const result = []
                source_ids.forEach(source_id => {
                    let source_data = rows
                        .filter(r => r.source_id = source_id)
                        .reduce((out,row) => {
                            out.geo.features.push({
                                type: 'Feature',
                                id: row.area_id,
                                properties: {
                                    area: row.area_id,
                                    name: row.name
                                },
                                geometry: row.geom
                            })
                            out.data[row.name] = row.value
                            return out
                        }, {
                            geo: {type: 'FeatureCollection', features:[]},
                            data: {}
                        })
                    let data = Object.keys(source_data.data)
                    .reduce((out, tazId) => {
                        out[tazId] = Object.values(source_data.data[tazId])
                            .reduce((viewOut, view) => {
                                let viewId = Object.keys(view)[0]
                                viewOut[viewId] = Object.values(view[viewId])
                                    .reduce((years,year) => {
                                        let yearKey = Object.keys(year)[0]
                                        years[yearKey] = year[yearKey]
                                        return years
                                    },{})
                                return viewOut
                            },{})
                        return out
                    },{})
                    result.push({
                      path: ["tig", "sed_taz", "bySource", source_id, "data"],
                      value: $atom({geo:source_data.geo, data}) ,
                    })

                }) 
                
                return result;
            });
        },
    },
];
