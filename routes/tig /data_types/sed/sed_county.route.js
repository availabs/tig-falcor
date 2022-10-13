const falcorJsonGraph = require("falcor-json-graph"),
    get = require('lodash.get'),
    $atom = falcorJsonGraph.atom,
    SedController = require("./sed.controller"),
    $ref = falcorJsonGraph.ref;
   

module.exports = [
     {
        route: `tig.sed_county.bySource[{keys:source_ids}].data`,
        get: function(pathSet) {
            const source_ids = pathSet.source_ids;
            return SedController.sedCountyBySourceDataMem.then(fn => fn(source_ids)).then((rows) => {
                const result = []
                source_ids.forEach(source_id => {
                    let source_data = rows
                        .filter(r => r.source_id = source_id)
                        .reduce((out,row) => {
                            out.data[row.name] = {value: row.value, enclosing_name: row.name}
                            return out
                        }, {
                            data: {}
                        })
                    let data = Object.keys(source_data.data)
                    .reduce((out, countyId) => {
                        out[countyId] = {
                            value: Object.values(source_data.data[countyId].value)
                                .reduce((viewOut, view) => {
                                    let viewId = Object.keys(view)[0]
                                    viewOut[viewId] = Object.values(view[viewId])
                                        .reduce((years, year) => {
                                            let yearKey = Object.keys(year)[0]
                                            years[yearKey] = year[yearKey]
                                            return years
                                        }, {})
                                    return viewOut
                                }, {}),
                            enclosing_name: source_data.data[countyId].enclosing_name
                        }
                        return out
                    },{})
                    result.push({
                      path: ["tig", "sed_county", "bySource", source_id, "data"],
                      value: $atom({data}) ,
                    })

                }) 
                
                return result;
            });
        },
    },
    {
        route: `tig.sed_county.bySource[{keys:source_ids}].geom`,
        get: function(pathSet) {
            const source_ids = pathSet.source_ids;
            return SedController.sedCountyBySourceGeomMem.then(fn => fn(source_ids)).then((rows) => {
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
                    .reduce((out, countyId) => {
                        out[countyId] = Object.values(source_data.data[countyId])
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
                      path: ["tig", "sed_county", "bySource", source_id, "geom"],
                      value: $atom({geo:source_data.geo, data}) ,
                    })

                })

                return result;
            });
        },
    },
];
