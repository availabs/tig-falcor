const falcorJsonGraph = require("falcor-json-graph"),
    get = require('lodash.get'),
    $atom = falcorJsonGraph.atom,
    HubBoundController = require("./hubBound.controller"),
    $ref = falcorJsonGraph.ref;
   

module.exports = [
     {
        route: `tig.hub_bound_travel_data.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            console.log('get hub bound data')
            console.time('hubbound query')
            return HubBoundController.tigHubBoundTravelDatabyViewIDMem.then(fn => fn(views)).then((rows) => {
                console.timeEnd('hubbound query')
                const result = [];
                console.time('hubbound process')
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "hub_bound_travel_data", "byId", viewID, 'data_overlay'],
                            value: $atom([]),
                        });
                    } else {
                        result.push({
                            path: ["tig", "hub_bound_travel_data", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                console.timeEnd('hubbound process')
                return result;
            });
        },
    },

    {
        route: `tig.hub_bound_travel_data.byId[{keys:views}].byYear[{keys:year}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            const years = pathSet.year;
            console.log('get hub bound data')
            console.time('hubbound query')
            return HubBoundController.tigHubBoundTravelDatabyViewIDMem.then(fn => fn(views, years)).then((rows) => {
                console.timeEnd('hubbound query')
                const result = [];
                console.time('hubbound process')
                views.forEach((viewID) => {
                    years.forEach(year => {
                        const filteredRows = rows.filter(r => r.view_id === viewID && r.year === year);

                        if (!filteredRows) {
                            result.push({
                                path: ["tig", "hub_bound_travel_data", "byId", viewID, 'byYear', year, 'data_overlay'],
                                value: $atom([]),
                            });
                        } else {
                            result.push({
                                path: ["tig", "hub_bound_travel_data", "byId", viewID, 'byYear', year, 'data_overlay'],
                                value: $atom(filteredRows),
                            });
                        }
                    })

                });
                console.timeEnd('hubbound process')
                return result;
            });
        },
    },

    {
        route: `tig.hub_bound_travel_data.byId[{keys:views}].years`,
        get: function(pathSet) {
            const views = pathSet.views;
            return HubBoundController.tigHubBoundYearsbyViewID(views)
                .then((rows) => {
                    const result = [];
                    views.forEach((viewID) => {
                        const filteredRows = rows.filter(r => r.view_id === viewID).map(r => r.years);

                        if (!filteredRows) {
                            result.push({
                                path: ["tig", "hub_bound_travel_data", "byId", viewID, 'years'],
                                value: $atom([]),
                            });
                        } else {
                            result.push({
                                path: ["tig", "hub_bound_travel_data", "byId", viewID, 'years'],
                                value: $atom(filteredRows[0]),
                            });
                        }
                    });
                    return result;
            });
        },
    },
];
