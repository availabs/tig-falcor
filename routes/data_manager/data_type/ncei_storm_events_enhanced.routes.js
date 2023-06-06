/* eslint object-shorthand: 0, func-names: 0, prefer-object-spread: 0 */
const {atom: $atom, ref: $ref} = require("falcor-json-graph");
const _ = require("lodash");

const {
    numRows,
    eventsByYear,
    eventsByType,
    enhancedNCEILossByYearByType,
    eventsMappingToNRICategories,
    enhancedNCEILossByGeoid,
    enhancedNCEINormalizedCount
} = require("../controller/data_type_controller");

module.exports = [
    {
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].numRows`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    const rows = await numRows(pgEnv, sourceIds, viewIds);

                    for (const sourceId of sourceIds) {
                        for (const viewId of viewIds) {
                            result.push({
                                path: ['ncei_storm_events_enhanced', pgEnv, 'source', sourceId, 'view', viewId, 'numRows'],
                                value: $atom(
                                    _.get(rows.find(
                                        r => r.source_id === sourceId && r.view_id === viewId
                                    ), 'num_rows')
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
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].eventsByYear`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    const rows = await eventsByYear(pgEnv, sourceIds, viewIds);

                    for (const sourceId of sourceIds) {
                        for (const viewId of viewIds) {
                            result.push({
                                path: ['ncei_storm_events_enhanced', pgEnv, 'source', sourceId, 'view', viewId, 'eventsByYear'],
                                value: $atom(
                                    _.get(rows.find(
                                        r => r.source_id === sourceId && r.view_id === viewId
                                    ), 'eventsByYear')
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
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].eventsByType`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    const rows = await eventsByType(pgEnv, sourceIds, viewIds, 'nri_category');

                    for (const sourceId of sourceIds) {
                        for (const viewId of viewIds) {
                            result.push({
                                path: ['ncei_storm_events_enhanced', pgEnv, 'source', sourceId, 'view', viewId, 'eventsByType'],
                                value: $atom(
                                    _.get(rows.find(
                                        r => r.source_id === sourceId && r.view_id === viewId
                                    ), 'eventsByType')
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
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].eventsMappingToNRICategories`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    const rows = await eventsMappingToNRICategories(pgEnv, sourceIds, viewIds);

                    for (const sourceId of sourceIds) {
                        for (const viewId of viewIds) {
                            result.push({
                                path: ['ncei_storm_events_enhanced', pgEnv, 'source', sourceId, 'view', viewId, 'eventsMappingToNRICategories'],
                                value: $atom(
                                    _.get(rows.find(
                                        r => r.source_id === sourceId && r.view_id === viewId
                                    ), 'eventsByType')
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
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].lossByYearByType`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    for (const sourceId of sourceIds) {
                        for (const viewId of viewIds) {
                            const rows = await enhancedNCEILossByYearByType(pgEnv, sourceId, viewId, 'nri_category');
                            result.push({
                                path: ['ncei_storm_events_enhanced', pgEnv, 'source', sourceId, 'view', viewId, 'lossByYearByType'],
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
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].source[{keys:sourceIds}].view[{keys:viewIds}].lossByGeoid[{keys:geoids}][{keys:groupBy}]`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds, geoids, groupBy} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    for (const sourceId of sourceIds) {
                        for (const viewId of viewIds) {
                            for (const geoid of geoids) {
                                for (const gb of groupBy) {
                                    const rows = await enhancedNCEILossByGeoid(pgEnv, sourceId, viewId, [geoid], gb);
                                    const filteredRes =  _.get(rows.find(r => r.source_id === sourceId && r.view_id === viewId), 'rows', [])

                                    result.push({
                                        path: ['ncei_storm_events_enhanced', pgEnv, 'source', sourceId, 'view', viewId, 'lossByGeoid', geoid, gb],
                                        value: $atom(filteredRes)
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
    {
        route: `ncei_storm_events_enhanced[{keys:pgEnvs}].view[{keys:viewIds}].normalized_event_count`,
        get: async function (pathSet) {
            try {
                const {pgEnvs, sourceIds, viewIds, geoids, groupBy} = pathSet;
                const result = [];

                for (const pgEnv of pgEnvs) {
                    for (const viewId of viewIds) {
                        const rows = await enhancedNCEINormalizedCount(pgEnv, viewId);
                        const filteredRes =  _.get(rows.find(r => r.view_id === viewId), 'rows', [])

                        result.push({
                            path: ['ncei_storm_events_enhanced', pgEnv,'view', viewId, 'normalized_event_count'],
                            value: $atom(filteredRes)
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


];