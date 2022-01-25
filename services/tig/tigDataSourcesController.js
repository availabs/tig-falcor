const db_service = require("../tig_db");

const SOURCES_ATTRIBUTES = [
    'id',
    'name',
    'description',
    'current_version',
    'data_starts_at',
    'data_ends_at',
    'origin_url',
    'user_id',
    'rows_updated_at',
    'rows_updated_by_id',
    'topic_area',
    'source_type'
]

const VIEWS_ATTRIBUTES = [
    'id',
    'name',
    'description',
    'source_id',
    'current_version',
    'data_starts_at',
    'data_ends_at',
    'origin_url',
    'user_id',
    'rows_updated_at',
    'rows_updated_by_id',
    'topic_area',
    'download_count',
    'last_displayed_at',
    'view_count',
    'created_at',
    'updated_at',
    'columns',
    'data_model',
    'statistic_id',
    'column_types',
    'data_levels',
    'value_name',
    'column_labels',
    'row_name',
    'column_name',
    'spatial_level',
    'data_hierarchy',
    'geometry_base_year',
    'deleted_at',
    'download_instructions',
    'value_columns',
    'short_description',
    'layer'
]
const tigDataSourcesByLength = () => {
    const sql = `
        SELECT count(id) as length
        FROM public.sources
    `;

    return db_service.promise(sql)
        .then(row => ({ ...row[0] }))
}

const tigDataSourcesByIndex = () =>{
    const sql = `
			SELECT id
			FROM public.sources
		`;
    return db_service.promise(sql);
}

const tigDataSourcesById = (ids) =>{
    const sql = `
    SELECT * 
    FROM public.sources
    WHERE id IN ('${ids.join(`','`)}')
    `;
    return db_service.promise(sql);

}

const tigDataSourcesViewsByLength = (source_ids) => {
    const sql = `
        SELECT source_id,COUNT(1)
        FROM public.views
        WHERE source_id IN ('${source_ids.join(`','`)}')
        GROUP BY source_id
    `;

    return db_service.promise(sql)

}

const tigDataSourcesViewsByIndex = (source_ids) =>{
    const sql = `
        SELECT id,source_id
        FROM public.views
        WHERE source_id IN ('${source_ids.join(`','`)}')
		`;
    return db_service.promise(sql);
}

const tigDataSourceViewsById = (source_ids,ids) =>{
    const sql = `
    SELECT * FROM public.views
    WHERE  source_id IN ('${source_ids.join(`','`)}')
    AND id IN  ('${ids.join(`','`)}')
    `
    return db_service.promise(sql);
}

const tigLayerByViewId = (viewIds) =>{
    const sql = `
    SELECT * FROM public.views
    WHERE id IN ('${viewIds.join(`','`)}')
    `
    return db_service.promise(sql);
}

const tigViewByLayer = (layer) =>{
    const sql = `
    SELECT distinct id, name, layer FROM public.views
    WHERE layer IN ('${layer.join(`','`)}')
    `
    return db_service.promise(sql);
}

const tigACSbyViewID = (viewIDs) =>{
    const sql = `
        SELECT view_id,
               areas.name                                area,
               lpad(fips_code::text, 11, '0')            fips,
               value,
               base_value,
               ("value" / NULLIF("base_value", 0)) * 100 percentage,
               type,
               geom
        FROM public.comparative_facts c
                 JOIN (
            SELECT a.*, st_asgeojson(geom) geom
            FROM public.areas a
                     JOIN public.base_geometries bg
                          ON base_geometry_id = bg.id
        ) areas
                      ON areas.id = area_id
        where view_id IN ('${viewIDs.join(`','`)}')
    `
    return db_service.promise(sql);
}

module.exports = {
    SOURCES_ATTRIBUTES,
    VIEWS_ATTRIBUTES,
    tigDataSourcesByLength,
    tigDataSourcesByIndex,
    tigDataSourcesById,
    tigDataSourcesViewsByLength,
    tigDataSourcesViewsByIndex,
    tigDataSourceViewsById,
    tigLayerByViewId,
    tigViewByLayer,
    tigACSbyViewID
}
