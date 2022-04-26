const falcorJsonGraph = require("falcor-json-graph"),
    get = require('lodash.get'),
    $atom = falcorJsonGraph.atom,
    TigDataSourcesService = require("../../services/tig/tigDataSourcesController"),
    $ref = falcorJsonGraph.ref,
    SOURCES_ATTRIBUTES = TigDataSourcesService.SOURCES_ATTRIBUTES,
    VIEWS_ATTRIBUTES = TigDataSourcesService.VIEWS_ATTRIBUTES;

module.exports = [
    {
        route: `tig.datasources.length`,
        get: function(pathSet) {
            console.time("tig.datasources.length");
            return TigDataSourcesService.tigDataSourcesByLength().then(
                (rows) => {
                    console.timeEnd("tig.datasources.length");
                    return [
                        {
                            path: ["tig", "datasources", "length"],
                            value: +rows.length,
                        },
                    ];
                }
            );
        },
    },
    {
        route: `tig.datasources.byIndex[{integers:indices}]`,
        get: function(pathSet) {
            return TigDataSourcesService.tigDataSourcesByIndex().then(
                (rows) => {
                    const result = [];
                    pathSet.indices.forEach((index) => {
                        const row = rows[index];
                        if (!row) {
                            result.push({
                                path: ["tig", "datasources", "byIndex", index],
                                value: null,
                            });
                        } else {
                            result.push({
                                path: ["tig", "datasources", "byIndex", index],
                                value: $ref([
                                    "tig",
                                    "datasources",
                                    "byId",
                                    row.id,
                                ]),
                            });
                        }
                    });
                    return result;
                }
            );
        },
    },
    {
        route: `tig.datasources.byId[{keys:ids}]['${SOURCES_ATTRIBUTES.join(
            "','"
        )}']`,
        get: function(pathSet) {
            const IDs = pathSet.ids;
            return TigDataSourcesService.tigDataSourcesById(IDs).then(
                (rows) => {
                    const result = [];
                    IDs.forEach((id) => {
                        const row = rows.reduce(
                            (a, c) =>
                                c.id.toString() === id.toString() ? c : a,
                            null
                        );
                        if (!row) {
                            result.push({
                                path: ["tig", "datasources", "byId", id],
                                value: $atom(null),
                            });
                        } else {
                            pathSet[4].forEach((attribute) => {
                                result.push({
                                    path: [
                                        "tig",
                                        "datasources",
                                        "byId",
                                        id,
                                        attribute,
                                    ],
                                    value: $atom(row[attribute]),
                                });
                            });
                        }
                    });
                    return result;
                }
            );
        },
    },
    {
        route: `tig.datasources.views.sourceId[{keys:ids}].length`,
        get: function(pathSet) {
            const source_ids = pathSet.ids;
            return TigDataSourcesService.tigDataSourcesViewsByLength(
                source_ids
            ).then((rows) => {
                const result = [];
                source_ids.forEach((source_id) => {
                    const row = rows.reduce(
                        (a, c) =>
                            c.source_id.toString() === source_id.toString()
                                ? c
                                : a,
                        null
                    );
                    if (!row) {
                        result.push({
                            path: [
                                "tig",
                                "datasources",
                                "views",
                                "sourceId",
                                source_id,
                                "length",
                            ],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: [
                                "tig",
                                "datasources",
                                "views",
                                "sourceId",
                                source_id,
                                "length",
                            ],
                            value: $atom(row.count),
                        });
                    }
                });
                return result;
            });
        },
    },
    {
        route: `tig.datasources.views.sourceId[{keys:ids}].byIndex[{integers:indices}]`,
        get: function(pathSet) {
            const source_ids = pathSet.ids;

            return TigDataSourcesService.tigDataSourcesViewsByIndex(
                source_ids
            ).then((rows) => {
                const result = [];
                source_ids.forEach((source_id) => {
                    const filteredRows = rows.reduce((a, c) => {
                        if (c.source_id.toString() === source_id.toString()) {
                            a.push(c);
                        }
                        return a;
                    }, []);
                    pathSet.indices.forEach((index) => {
                        const row = filteredRows[index];
                        if (!row) {
                            result.push({
                                path: [
                                    "tig",
                                    "datasources",
                                    "views",
                                    "sourceId",
                                    source_id,
                                    "byIndex",
                                    index,
                                ],
                                value: $atom(null),
                            });
                        } else {
                            result.push({
                                path: [
                                    "tig",
                                    "datasources",
                                    "views",
                                    "sourceId",
                                    source_id,
                                    "byIndex",
                                    index,
                                ],
                                value: $ref([
                                    "tig",
                                    "datasources",
                                    "views",
                                    "sourceId",
                                    source_id,
                                    "byId",
                                    row.id,
                                ]),
                            });
                        }
                    });
                });
                return result;
            });
        },
    },
    {
        route: `tig.datasources.views.sourceId[{keys:source_ids}].byId[{keys:ids}]['${VIEWS_ATTRIBUTES.join(
            "','"
        )}']`,
        get: function(pathSet) {
            const ids = pathSet.ids;
            const source_ids = pathSet.source_ids;
            return TigDataSourcesService.tigDataSourceViewsById(
                source_ids,
                ids
            ).then((rows) => {
                const result = [];
                ids.forEach((id) => {
                    source_ids.forEach((source_id) => {
                        const row = rows.reduce(
                            (a, c) =>
                                c.id.toString() === id.toString() &&
                                c.source_id.toString() === source_id.toString()
                                    ? c
                                    : a,
                            null
                        );
                        if (!row) {
                            result.push({
                                path: [
                                    "tig",
                                    "datasources",
                                    "views",
                                    "sourceId",
                                    source_id,
                                    "byId",
                                    id,
                                ],
                                value: $atom(null),
                            });
                        } else {
                            pathSet[7].forEach((attribute) => {
                                result.push({
                                    path: [
                                        "tig",
                                        "datasources",
                                        "views",
                                        "sourceId",
                                        source_id,
                                        "byId",
                                        id,
                                        attribute,
                                    ],
                                    value: $atom(row[attribute]),
                                });
                            });
                        }
                    });
                });
                return result;
            });
        },
    },
    {
        route: `tig.byViewId[{keys:view_ids}]['${VIEWS_ATTRIBUTES.join("','")}']`,
        get: function(pathSet) {
            const viewIds = pathSet.view_ids;
            return TigDataSourcesService.tigLayerByViewId(viewIds).then((rows) => {
                const result = [];
                viewIds.forEach((id) => {
                    const row = rows.reduce((a, c) => c.id.toString() === id.toString() ? c : a, null);

                    if (!row) {
                        result.push({
                            path: ["tig", "byViewId", id],
                            value: $atom(null),
                        });
                    } else {
                        pathSet[3].forEach((attribute) => {
                            result.push({
                                path: ["tig", "byViewId", id, attribute,],
                                value: $atom(row[attribute]),
                            });
                        });
                    }
                });
                return result;
            });
        },
    },
    {
        route: `tig.views.byLayer[{keys:layers}]`,
        get: function(pathSet) {
            const layers = pathSet.layers;
            return TigDataSourcesService.tigViewByLayer(layers).then((rows) => {
                const result = [];
                layers.forEach((layer) => {
                    const filteredRows = rows.filter(r => r.layer === layer);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "views", "byLayer", layer],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "views", "byLayer", layer],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },

    {
        route: `tig.acs_census.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigACSbyViewIDMem.then(fn => fn(views)).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "acs_census", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "acs_census", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },

    {
        route: `tig.sed_county.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigSEDCountybyViewID(views).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "sed_county", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "sed_county", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },

    {
        route: `tig.sed_county_2055.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigSEDCounty2055byViewID(views).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "sed_county_2055", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "sed_county_2055", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },

    {
        route: `tig.rtp_project_data.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigRTPProjectsbyViewID(views).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "rtp_project_data", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "rtp_project_data", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },

    {
        route: `tig.tip.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigTipbyViewID(views).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "tip", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "tip", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },

    {
        route: `tig.sed_taz.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            console.time('SED TAZ Data Fetch')
            return TigDataSourcesService.tigSEDTazbyViewID(views).then((rows) => {
                const result = [];
                console.timeEnd('SED TAZ Data Fetch')
                console.time('SED TAZ Data process')
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "sed_taz", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "sed_taz", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                console.timeEnd('SED TAZ Data process')
                return result;
            });
        },
    },

    {
        route: `tig.hub_bound_travel_data.byId[{keys:views}].data_overlay`,
        get: function(pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigHubBoundTravelDatabyViewID(views).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "hub_bound_travel_data", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
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

    {
        route: `tig.bpm_performance.byId[{keys:views}].data_overlay`,
        get: function (pathSet) {
            const views = pathSet.views;
            return TigDataSourcesService.tigBPMPerformancebyViewID(views).then((rows) => {
                const result = [];
                views.forEach((viewID) => {
                    const filteredRows = rows.filter(r => r.view_id === viewID);

                    if (!filteredRows) {
                        result.push({
                            path: ["tig", "bpm_performance", "byId", viewID, 'data_overlay'],
                            value: $atom(null),
                        });
                    } else {
                        result.push({
                            path: ["tig", "bpm_performance", "byId", viewID, 'data_overlay'],
                            value: $atom(filteredRows),
                        });
                    }
                });
                return result;
            });
        },
    },
    {
        route: `tig.source[{keys:source}].view[{keys:view}]`,
        get: function(pathSet) {
            return TigDataSourcesService.viewData(pathSet.source[0], pathSet.view).then((rows) => {
                const response = []
                pathSet.source.forEach(source => {
                    pathSet.view.forEach(view => {
                        let filteredRows =
                            source.includes('acs') ?
                                rows.filter(r => r[view]).map(r => r[view])
                                    .reduce((acc, r) => ({...acc, ...r}), {}) :
                                rows.filter(r => r[view]).map(r => r[view])[0]
                                    .reduce((acc, r) => ({...acc, ...r}), {})
                        response.push(
                            {
                                path: ["tig", 'source', source, 'view', view],
                                value: $atom(filteredRows),
                            }
                        )
                    })
                })
                return response
            });
        },
    },

    {
        route: `tig.geoms.gid[{keys:ids}]`,
        get: function(pathSet) {
            return TigDataSourcesService.geomsMem.then((fn => fn(pathSet.ids))).then((rows) => {
                const response = []
                pathSet.ids.forEach(id => {
                    let filteredRows = rows.filter(r => r.id === id)
                    response.push(
                        {
                            path: ["tig", 'geoms', 'gid', id],
                            value: get(filteredRows, [0, 'geom']),
                        }
                    )
                })
                return response
            });
        },
    },
];
