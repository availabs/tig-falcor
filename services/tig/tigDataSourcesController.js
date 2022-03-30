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
        WHERE source_id IN ('${source_ids.join(`','`)}') and deleted_at is null
        GROUP BY source_id
    `;

    return db_service.promise(sql)

}

const tigDataSourcesViewsByIndex = (source_ids) =>{
    const sql = `
        SELECT id,source_id
        FROM public.views
        WHERE source_id IN ('${source_ids.join(`','`)}') and deleted_at is null
		`;
    return db_service.promise(sql);
}

const tigDataSourceViewsById = (source_ids,ids) =>{
    const sql = `
    SELECT * FROM public.views
    WHERE  source_id IN ('${source_ids.join(`','`)}') and deleted_at is null
    AND id IN  ('${ids.join(`','`)}')
    `
    return db_service.promise(sql);
}

const tigLayerByViewId = (viewIds) =>{
    const sql = `
    SELECT * FROM public.views
    WHERE id IN ('${viewIds.join(`','`)}') and deleted_at is null
    `
    return db_service.promise(sql);
}

const tigViewByLayer = (layer) =>{
    const sql = `
    SELECT distinct v.id, v.name, layer, s.name source_name 
    FROM public.views v
             join sources s
                  on source_id = s.id
    WHERE layer IN ('${layer.join(`','`)}') and deleted_at is null
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

const tigSEDCountybyViewID = (viewIDs) =>{
    const sql = `
        SELECT areas.name area, areas.type, view_id, json_object_agg(df.year, value) AS data, geom
        FROM public.demographic_facts df
                 JOIN (
            SELECT a.*, st_asgeojson(geom) geom
            FROM public.areas a
                     JOIN public.base_geometries bg
                          ON base_geometry_id = bg.id
        ) areas
                      ON area_id = areas.id
        where view_id IN ('${viewIDs.join(`','`)}')
        group by 1, 2, 3, 5
    `;

    return db_service.promise(sql);
}

const tigSEDCounty2055byViewID = (viewIDs) =>{
    const sql = `
        SELECT enclosing_name area, enclosing_type AS type, view_id, json_object_agg(df.year, value) AS data, geom
        FROM public.demographic_facts df
                 JOIN (
                        SELECT a.*, st_asgeojson(geom) geom
                        FROM public.areas a
                                 JOIN public.base_geometries bg
                                      ON base_geometry_id = bg.id
                    ) areas
                ON area_id = areas.id
                 JOIN (
                        SELECT name enclosing_name, type enclosing_type, enclosed_area_id
                        FROM public.area_enclosures
                                 join areas
                                      on areas.id = enclosing_area_id
                    ) enclosing_geoms
                ON enclosed_area_id = area_id
        where view_id IN ('${viewIDs.join(`','`)}')
        group by 1, 2, 3, 5
    `;
    return db_service.promise(sql);
}

const tigRTPProjectsbyViewID = (viewIDs) =>{
    const sql = `
        SELECT view_id, rtp.description, estimated_cost, p.name plan_portion, pt.name ptype, rtp_id, s.name sponsor, rtp.year, a.name, st_asGeoJson(geography) geom, geography
        FROM public.rtp_projects rtp
                 JOIN plan_portions p
                      ON plan_portion_id = p.id
                 JOIN ptypes pt
                      ON ptype_id = pt.id
                 JOIN sponsors s
                      ON sponsor_id = s.id
                 JOIN areas a
                      ON county_id = a.id
        WHERE view_id IN ('${viewIDs.join(`','`)}')
    `;

    return db_service.promise(sql);
}

const tigTipbyViewID = (viewIDs) =>{
    const sql = `
        SELECT view_id,
               tip_id,
               cost,
               rtp.description,
               m.name               mpo,
               pt.name                 ptype,
               s.name                  sponsor,
               a.name,
               st_asGeoJson(geography) geom,
               geography
        FROM public.tip_projects rtp
                 JOIN mpos m
                      ON mpo_id = m.id
                 JOIN ptypes pt
                      ON ptype_id = pt.id
                 JOIN sponsors s
                      ON sponsor_id = s.id
                 JOIN areas a
                      ON county_id = a.id
        WHERE view_id IN ('${viewIDs.join(`','`)}')
    `;

    return db_service.promise(sql);
}

const tigSEDTazbyViewID = (viewIDs) =>{
    const sql = `
        SELECT q.*, st_asgeojson(bg.geom) as geom from 
        (SELECT areas.name area, areas.type, view_id, json_object_agg(df.year, value) AS data, enclosing_name, enclosing_type, bg_id
        FROM public.demographic_facts df
                 JOIN (
                        SELECT a.*, bg.id as bg_id
                        FROM public.areas a
                                 JOIN public.base_geometries bg
                                      ON base_geometry_id = bg.id
                    ) areas
                ON area_id = areas.id
                 JOIN (
                        SELECT name enclosing_name, type enclosing_type, enclosed_area_id
                        FROM public.area_enclosures
                                 join areas
                                      on areas.id = enclosing_area_id
                    ) enclosing_geoms
                ON enclosed_area_id = area_id
        where view_id IN (${viewIDs.join(`','`)})
        group by 1, 2, 3, 5, 6,7) as q
        join public.base_geometries bg on q.bg_id = bg.id
    `;

    return db_service.promise(sql);
}

const tigHubBoundTravelDatabyViewID = (viewIDs) =>{
    const sql = `
        SELECT count,
               hour,
               cf.id,
               l.latitude  lat,
               l.longitude lon,
               location_id loc_id,
               l.name      loc_name,
               m.name      mode_name,
               tr.name     route_name,
               s.name      sector_name,
               cv.name     var_name,
               ta.name     transit_agency,
               year,
               direction,
               view_id
        FROM public.count_facts cf
                 JOIN count_variables cv
                      ON count_variable_id = cv.id
                 JOIN sectors s
                      ON sector_id = s.id
                 JOIN transit_routes tr
                      ON transit_route_id = tr.id
                 JOIN transit_modes m
                      ON transit_mode_id = m.id
                 JOIN locations l
                      ON location_id = l.id
                 JOIN transit_agencies ta
                      ON cf.transit_agency_id = ta.id
        where view_id IN ('${viewIDs.join(`','`)}')
    `;

    return db_service.promise(sql);
}

const tigBPMPerformancebyViewID = (viewIDs) =>{
    const sql = `
        SELECT a.name,
               a.type,
               avg_speed,
               functional_class,
               period,
               vehicle_miles_traveled,
               vehicle_hours_traveled,
               geom,
               view_id
        FROM public.performance_measures_facts
        JOIN (
            SELECT a.*, st_asgeojson(geom) geom
                        FROM public.areas a
                                 JOIN public.base_geometries bg
                                      ON base_geometry_id = bg.id
            ) a
                      ON a.id = area_id
        where view_id IN ('${viewIDs.join(`','`)}')
    `;

    return db_service.promise(sql);
}

const viewData = (source, viewIDs) =>{
    const sql = `
        SELECT ${viewIDs.map(v => `"${v}"`).join(',')}
        FROM datatable_${source.toLowerCase().split(' ').join('_')}_data
    `;

    return db_service.promise(sql);
}

const geoms = (ids) =>{
    const sql = `
        SELECT id, st_asgeojson(geom) as geom
        FROM public.base_geometries 
        WHERE id IN (${ids})
    `;

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
    tigACSbyViewID,
    tigSEDCountybyViewID,
    tigSEDTazbyViewID,
    tigSEDCounty2055byViewID,
    tigRTPProjectsbyViewID,
    tigTipbyViewID,
    tigHubBoundTravelDatabyViewID,
    tigBPMPerformancebyViewID,
    viewData,
    geoms
}
