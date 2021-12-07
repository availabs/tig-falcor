const falcorJsonGraph = require("falcor-json-graph"),
    $atom = falcorJsonGraph.atom,
    TigDataSourcesService = require("../../services/tig/tigDataSourcesController"),
    $ref = falcorJsonGraph.ref,
    SOURCES_ATTRIBUTES = TigDataSourcesService.SOURCES_ATTRIBUTES,
    VIEWS_ATTRIBUTES = TigDataSourcesService.VIEWS_ATTRIBUTES;

module.exports = [
    {
        route: `tig.datasources.length`,
        get: function(pathSet) {
            console.log("tig.datasources.length");
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
];
