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
                return result;
            });
        },
    },
];
