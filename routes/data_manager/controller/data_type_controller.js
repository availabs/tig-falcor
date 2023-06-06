const pgFormat = require("pg-format");
const dedent = require("dedent");
const _ = require("lodash");

const { listPgEnvs, getDb } = require("../databases");

const { getViewDependencies } = require("./multi-db");

const SourceAttributes = [
  "source_id",
  "name",
  "display_name",
  "type",
  "update_interval",
  "category",
  "categories",
  "description",
  "statistics",
  "metadata",
];

const ViewAttributes = [
  "view_id",
  "source_id",
  "data_type",
  "interval_version",
  "geography_version",
  "version",
  "metadata",
  "source_url",
  "publisher",
  "data_table",
  "download_url",
  "tiles_url",
  "start_date",
  "end_date",
  "last_updated",
  "statistics",
];

const getTableFromIds = async (pgEnv, source_id, view_id) => {
  const sql = view_id === 'lts' ?
      `
        with t as (SELECT source_id, 'lts' view_id, table_schema, table_name, rank() over (partition by source_id order by _created_timestamp desc) 
        FROM data_manager.views
        where source_id = ${source_id})
        
        SELECT * FROM t where rank = 1;
      ` :
      `
       SELECT source_id, view_id, table_schema, table_name
       FROM data_manager.views
       where source_id = ${source_id}
       and view_id = ${view_id};
      `;

  const db = await getDb(pgEnv);

  return db.query(sql);

}
const getTableFromViewId = async (pgEnv, view_id) => {
  const sql =
      `
       SELECT source_id, view_id, table_schema, table_name
       FROM data_manager.views
       where view_id = ${view_id};
      `;

  const db = await getDb(pgEnv);

  return db.query(sql);

}

const getLtsView = async (pgEnv, source_id) => {
  const sql =
      `
        with t as (SELECT source_id, view_id, table_schema, table_name, rank() over (partition by source_id order by _created_timestamp desc) 
        FROM data_manager.views
        where source_id = ${source_id})
        
        SELECT * FROM t where rank = 1;
      `;

  const db = await getDb(pgEnv);

  const {rows} = await db.query(sql);

  return rows;

}
const numRows = async (pgEnv, source_ids, view_ids) => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = await table_names.rows.reduce(async (acc, table) => {
    await acc;

    const numRowsSql = `SELECT count(1) num_rows FROM ${table.table_schema}.${table.table_name}`;

    const {
      rows: [{ num_rows }]
    } = await db.query(numRowsSql);

    acc.push({ ...table, num_rows });

    return acc;
  }, []);

  return res;
};

const eventsByYear = async (pgEnv, source_ids, view_ids) => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `SELECT year, count(1) num_events FROM ${table.table_schema}.${table.table_name} group by year`;

    const {
      rows: eventsByYear
    } = await db.query(sql);

    acc.push({ ...table, eventsByYear });

    return acc;
  }, []);

  return res;
};

const eventsByType = async (pgEnv, source_ids, view_ids, typeCol = 'event_type') => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
                with t as (
                SELECT coalesce(${typeCol}, 'UNDEFINED') as event_type, count(1) total_events,
                       sum(
                           case
                             when coalesce(property_damage,0) + coalesce(crop_damage,0) + coalesce(injuries_direct,0) + coalesce(injuries_indirect,0) + coalesce(deaths_direct,0) + coalesce(deaths_indirect,0) > 0 then 1
                             else 0
                             end
                         )::integer as loss_events,
                         NULLIF(max(year) - min(year), 0) num_years, 
                         coalesce(sum(property_damage), 0)::double precision as property_damage, 
                         coalesce(sum(crop_damage), 0)::double precision as crop_damage,
                         coalesce(sum(
                           coalesce(deaths_direct::float,0) +
                           coalesce(deaths_indirect::float,0) +
                           (
                               (
                                   coalesce(injuries_direct::float,0) +
                                   coalesce(injuries_indirect::float,0)
                                 ) / 10
                             )
                         ), 0) * 11600000   as person_damage
                
                FROM ${table.table_schema}.${table.table_name} 
                group by 1)

                
                SELECT event_type, num_years,
                       total_events, loss_events,
                       property_damage, crop_damage, person_damage,
                       property_damage / num_years annual_property_damage,
                       crop_damage / num_years annual_crop_damage,
                       person_damage / num_years annual_person_damage
                FROM t;
                `;

    const {
      rows: eventsByType
    } = await db.query(sql);

    acc.push({ ...table, eventsByType });

    return acc;
  }, []);

  return res;
};

const eventsMappingToNRICategories = async (pgEnv, source_ids, view_ids) => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
                SELECT nri_category, ARRAY_AGG(distinct event_type order by event_type) event_types, count(1) num_events 
                FROM ${table.table_schema}.${table.table_name} group by 1`;

    const {
      rows: eventsByType
    } = await db.query(sql);

    acc.push({ ...table, eventsByType });

    return acc;
  }, []);

  return res;
};

const per_basis_stats = async (pgEnv, source_ids, view_ids) => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
          with t as (
          SELECT nri_category, ctype, count(1) num_events_total,
                  sum(CASE WHEN damage = 0 THEN 1 ELSE 0 END) num_events_zero_loss,
                  sum(CASE WHEN damage > 0 THEN 1 ELSE 0 END) num_events_with_loss,
                  sum(damage) damage,
                  sum(damage_adjusted) damage_adjusted
          FROM ${table.table_schema}.${table.table_name}
          GROUP BY 1, 2
          )

          SELECT nri_category,
                 sum(num_events_total) num_events_total, 
                 sum(num_events_zero_loss) num_events_zero_loss, 
                 sum(num_events_with_loss) num_events_with_loss,
                  sum(CASE WHEN ctype = 'buildings' THEN damage ELSE 0 END) damage_buildings,		
                  sum(CASE WHEN ctype = 'crop' THEN damage ELSE 0 END) damage_crop,		
                  sum(CASE WHEN ctype = 'population' THEN damage ELSE 0 END) damage_population,
                  sum(damage) damage_total,
                  sum(CASE WHEN ctype = 'buildings' THEN damage_adjusted ELSE 0 END) damage_adjusted_buildings,		
                  sum(CASE WHEN ctype = 'crop' THEN damage_adjusted ELSE 0 END) damage_adjusted_crop,		
                  sum(CASE WHEN ctype = 'population' THEN damage_adjusted ELSE 0 END) damage_adjusted_population,
                  sum(damage_adjusted) damage_adjusted_total
          FROM t
          GROUP BY 1;
    `;

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
}
// added new code here 
const ddCount= async (pgEnv, source_ids, view_ids) =>{
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  console.log('table name:', table_names);
  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `select fy_declared, count(1) disaster_number 
    FROM ${table.table_schema}.${table.table_name} 
    group by fy_declared`;
    console.log(sql)
    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;

}

const incidentByMonth= async (pgEnv, source_ids, view_ids) =>{
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  console.log('table name:', table_names);
  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `select fy_declared, count(1) disaster_number 
    FROM ${table.table_schema}.${table.table_name} 
    group by fy_declared`;
    console.log(sql)
    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;

}
// end of new code
const ealFromHlr = async (pgEnv, source_ids, view_ids) => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = await table_names.rows.reduce(async (acc, table) => {
    await acc;

    const hlrSql = `
      with dollars as (SELECT nri_category,
                              ctype,
                              CASE
                                WHEN ctype = 'buildings'
                                  THEN sum(CASE
                                             WHEN nri_category IN ('coastal')
                                               THEN hlr * CFLD_EXPB * CFLD_AFREQ
                                             WHEN nri_category IN ('coldwave')
                                               THEN hlr * CWAV_EXPB * CWAV_AFREQ
                                             WHEN nri_category IN ('hurricane')
                                               THEN hlr * HRCN_EXPB * HRCN_AFREQ
                                             WHEN nri_category IN ('heatwave')
                                               THEN hlr * HWAV_EXPB * HWAV_AFREQ
                                             WHEN nri_category IN ('hail')
                                               THEN hlr * HAIL_EXPB * HAIL_AFREQ
                                             WHEN nri_category IN ('tornado')
                                               THEN hlr * TRND_EXPB * TRND_AFREQ
                                             WHEN nri_category IN ('riverine')
                                               THEN hlr * RFLD_EXPB * RFLD_AFREQ
                                             WHEN nri_category IN ('lightning')
                                               THEN hlr * LTNG_EXPB * LTNG_AFREQ
                                             WHEN nri_category IN ('landslide')
                                               THEN hlr * LNDS_EXPB * LNDS_AFREQ
                                             WHEN nri_category IN ('icestorm')
                                               THEN hlr * ISTM_EXPB * ISTM_AFREQ
                                             WHEN nri_category IN ('wind')
                                               THEN hlr * SWND_EXPB * SWND_AFREQ
                                             WHEN nri_category IN ('wildfire')
                                               THEN (
                                                      CASE WHEN geoid IN (select stcofips
                                                                          FROM national_risk_index.nri_126
                                                                          where wfir_hlrb = 0.4)
                                                             THEN 0.4
                                                           ELSE hlr
                                                        END
                                                      ) * WFIR_EXPB * WFIR_AFREQ
                                             WHEN nri_category IN ('winterweat')
                                               THEN hlr * WNTW_EXPB * WNTW_AFREQ
                                             WHEN nri_category IN ('tsunami')
                                               THEN hlr * TSUN_EXPB * TSUN_AFREQ
                                             WHEN nri_category IN ('avalanche')
                                               THEN hlr * AVLN_EXPB * AVLN_AFREQ
                                             WHEN nri_category IN ('volcano')
                                               THEN hlr * VLCN_EXPB * VLCN_AFREQ
                                  END)
                                WHEN ctype = 'crop'
                                  THEN sum(CASE
                                             WHEN nri_category IN ('coldwave')
                                               THEN hlr * CWAV_EXPA * CWAV_AFREQ
                                             WHEN nri_category IN ('drought')
                                               THEN hlr * DRGT_EXPA * DRGT_AFREQ
                                             WHEN nri_category IN ('hurricane')
                                               THEN hlr * HRCN_EXPA * HRCN_AFREQ
                                             WHEN nri_category IN ('heatwave')
                                               THEN hlr * HWAV_EXPA * HWAV_AFREQ
                                             WHEN nri_category IN ('hail')
                                               THEN hlr * HAIL_EXPA * HAIL_AFREQ
                                             WHEN nri_category IN ('tornado')
                                               THEN hlr * TRND_EXPA * TRND_AFREQ
                                             WHEN nri_category IN ('riverine')
                                               THEN hlr * RFLD_EXPA * RFLD_AFREQ
                                             WHEN nri_category IN ('wind')
                                               THEN hlr * SWND_EXPA * SWND_AFREQ
                                             WHEN nri_category IN ('wildfire')
                                               THEN hlr * WFIR_EXPA * WFIR_AFREQ
                                             WHEN nri_category IN ('winterweat')
                                               THEN hlr * WNTW_EXPA * WNTW_AFREQ
                                  END)
                                WHEN ctype = 'population'
                                  THEN sum(CASE
                                             WHEN nri_category IN ('coastal')
                                               THEN hlr * CFLD_EXPPE * CFLD_AFREQ
                                             WHEN nri_category IN ('coldwave')
                                               THEN hlr * CWAV_EXPPE * CWAV_AFREQ
                                             WHEN nri_category IN ('hurricane')
                                               THEN hlr * HRCN_EXPPE * HRCN_AFREQ
                                             WHEN nri_category IN ('heatwave')
                                               THEN hlr * HWAV_EXPPE * HWAV_AFREQ
                                             WHEN nri_category IN ('hail')
                                               THEN hlr * HAIL_EXPPE * HAIL_AFREQ
                                             WHEN nri_category IN ('tornado')
                                               THEN hlr * TRND_EXPPE * TRND_AFREQ
                                             WHEN nri_category IN ('riverine')
                                               THEN hlr * RFLD_EXPPE * RFLD_AFREQ
                                             WHEN nri_category IN ('lightning')
                                               THEN hlr * LTNG_EXPPE * LTNG_AFREQ
                                             WHEN nri_category IN ('landslide')
                                               THEN hlr * LNDS_EXPPE * LNDS_AFREQ
                                             WHEN nri_category IN ('icestorm')
                                               THEN hlr * ISTM_EXPPE * ISTM_AFREQ
                                             WHEN nri_category IN ('wind')
                                               THEN hlr * SWND_EXPPE * SWND_AFREQ
                                             WHEN nri_category IN ('wildfire')
                                               THEN hlr * WFIR_EXPPE * WFIR_AFREQ
                                             WHEN nri_category IN ('winterweat')
                                               THEN hlr * WNTW_EXPPE * WNTW_AFREQ
                                             WHEN nri_category IN ('tsunami')
                                               THEN hlr * TSUN_EXPPE * TSUN_AFREQ
                                             WHEN nri_category IN ('avalanche')
                                               THEN hlr * AVLN_EXPPE * AVLN_AFREQ
                                             WHEN nri_category IN ('volcano')
                                               THEN hlr * VLCN_EXPPE * VLCN_AFREQ
                                  END)
                                END avail
                       FROM ${table.table_schema}.${table.table_name}
                              JOIN national_risk_index.nri_126
                                   ON geoid = stcofips
                       GROUP BY 1, 2

                       ORDER BY 1, 2)


      select nri_category,
             sum(case when ctype = 'buildings' then avail else 0 end)  swd_buildings,
             sum(case when ctype = 'crop' then avail else 0 end)       swd_crop,
             sum(case when ctype = 'population' then avail else 0 end) swd_population
      from dollars
      group by 1
      order by 1`;

    const {
      rows
    } = await db.query(hlrSql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const ealFromEal = async (pgEnv, source_ids, view_ids) => {
  const table_names = await getTableFromIds(pgEnv, source_ids, view_ids);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `SELECT * FROM ${table.table_schema}.${table.table_name}`;

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};


const enhancedNCEILossByYearByType = async (pgEnv, source_id, view_id, typeCol = 'event_type') => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
    SELECT year, nri_category, --nri_category, 
		sum(property_damage) property_damage, sum(crop_damage) crop_damage,
        sum(coalesce(property_damage, 0) + coalesce(crop_damage, 0)) total_damage,
        sum(property_damage_adjusted) property_damage_adjusted, sum(crop_damage_adjusted) crop_damage_adjusted,
        sum(coalesce(property_damage_adjusted, 0) + coalesce(crop_damage_adjusted, 0)) total_damage_adjusted
	FROM ${table.table_schema}.${table.table_name}
	WHERE nri_category not in 
	('Astronomical Low Tide', 'Dense Fog', 'Dust Devil', 'Dust Storm', 
	    'Heavy Rain', 'Marine Dense Fog', 'Dense Smoke', 'Freezing Fog', 'Northern Lights')
	GROUP BY 1, 2
	ORDER BY 1, 2`;

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const enhancedNCEILossByGeoid = async (pgEnv, source_id, view_id, geoid, groupBy) => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
    SELECT geoid, ${groupBy ? `${groupBy}, ` : ``}
		sum(property_damage) property_damage, sum(crop_damage) crop_damage,
        sum(coalesce(property_damage, 0) + coalesce(crop_damage, 0)) total_damage,
        sum(property_damage_adjusted) property_damage_adjusted, sum(crop_damage_adjusted) crop_damage_adjusted,
        sum(coalesce(property_damage_adjusted, 0) + coalesce(crop_damage_adjusted, 0)) total_damage_adjusted
	FROM ${table.table_schema}.${table.table_name}
	WHERE nri_category not in 
	('Astronomical Low Tide', 'Dense Fog', 'Dust Devil', 'Dust Storm', 
	    'Heavy Rain', 'Marine Dense Fog', 'Dense Smoke', 'Freezing Fog', 'Northern Lights')
	AND geoid in (${geoid.map(gid => `'${gid}'`).join(', ')})
	GROUP BY 1 ${groupBy ? `,2 ` : ``}
	ORDER BY 1 ${groupBy ? `,2 ` : ``};
    `

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const enhancedNCEINormalizedCount = async (pgEnv, view_id) => {
  const table_names = await getTableFromViewId(pgEnv, view_id);
  const db = await getDb(pgEnv);
  const startYear = 1996,
        endYear = 2022;
  const populationDollarAmt = 11600000;

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
    with buildings as (
    select 'buildings'   ctype,
            event_id,
            substring(geoid, 1, 5) geoid,
            nri_category,
            min(begin_date_time)::date     swd_begin_date,
            max(end_date_time) ::date      swd_end_date,
            coalesce(sum(property_damage), 0) damage
    FROM ${table.table_schema}.${table.table_name}
    WHERE year between ${startYear} and ${endYear}
    AND geoid is not null
    AND nri_category is not null
    GROUP BY 1, 2, 3, 4
        ),
        crop as (
    select
        'crop' ctype,
        event_id,
        substring (geoid, 1, 5) geoid,
        nri_category,
        min (begin_date_time):: date swd_begin_date,
        max (end_date_time):: date swd_end_date,
        coalesce (sum (crop_damage), 0) damage
    FROM ${table.table_schema}.${table.table_name}
    WHERE year between ${startYear} and ${endYear}
      AND geoid is not null
      AND nri_category is not null
    GROUP BY 1, 2, 3, 4
        ),
        population as (
    select
        'population' ctype,
        event_id,
        substring (geoid, 1, 5) geoid,
        nri_category,
        min (begin_date_time):: date swd_begin_date,
        max (end_date_time):: date swd_end_date,
        coalesce (
          sum (
            coalesce (deaths_direct:: float, 0) +
            coalesce (deaths_indirect:: float, 0) +
            (
              (
              coalesce (injuries_direct:: float, 0) +
              coalesce (injuries_indirect:: float, 0)
              ) / 10
            )
          )
        , 0) * ${populationDollarAmt} damage
    FROM ${table.table_schema}.${table.table_name}
    WHERE year between ${startYear} and ${endYear}
      AND geoid is not null
      AND nri_category is not null
    GROUP BY 1, 2, 3, 4
        ),
        alldata as (
          select * from buildings
          union all
          select * from crop
          union all
          select * from population
        )
    select nri_category,
           SUM(CASE WHEN damage > 0 THEN 1 ELSE 0 END)::integer loss_events,
           count(1)::integer                                    total_events
    FROM alldata
    group by 1
    order by 1;
    `

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const nriTotals = async (pgEnv, source_id, view_id) => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
      SELECT 
        sum(avln_ealt) avalanche,
                            sum(avln_ealb) avalanche_buildings,
                           sum(avln_ealpe) avalanche_population,

                           sum(cfld_ealt) coastal,
                           sum(cfld_ealb) coastal_buildings,
                           sum(cfld_ealpe) coastal_population,

                           sum(cwav_ealt) coldwave,
                           sum(cwav_eala) coldwave_crop,
                           sum(cwav_ealb) coldwave_buildings,
                           sum(cwav_ealpe) coldwave_population,

                           sum(drgt_ealt) drought,
                           sum(drgt_eala) drought_crop,

                           sum(erqk_ealt) earthquake,
                           sum(erqk_ealb) earthquake_buildings,
                           sum(erqk_ealpe) earthquake_population,

                           sum(hail_ealt) hail,
                           sum(hail_eala) hail_crop,
                           sum(hail_ealb) hail_buildings,
                           sum(hail_ealpe) hail_population,

                           sum(hwav_ealt) heatwave,
                           sum(hwav_eala) heatwave_crop,
                           sum(hwav_ealb) heatwave_buildings,
                           sum(hwav_ealpe) heatwave_population,

                           sum(hrcn_ealt) hurricane,
                           sum(hrcn_eala) hurricane_crop,
                           sum(hrcn_ealb) hurricane_buildings,
                           sum(hrcn_ealpe) hurricane_population,

                           sum(istm_ealt) icestorm,
                           sum(istm_ealb) icestorm_buildings,
                           sum(istm_ealpe) icestorm_population,

                           sum(lnds_ealt) landslide,
                           sum(lnds_ealb) landslide_buildings,
                           sum(lnds_ealpe) landslide_population,

                           sum(ltng_ealt) lightning,
                           sum(ltng_ealb) lightning_buildings,
                           sum(ltng_ealpe) lightning_population,

                           sum(rfld_ealt) riverine,
                           sum(rfld_eala) riverine_crop,
                           sum(rfld_ealb) riverine_buildings,
                           sum(rfld_ealpe) riverine_population,

                           sum(swnd_ealt) wind,
                           sum(swnd_eala) wind_crop,
                           sum(swnd_ealb) wind_buildings,
                           sum(swnd_ealpe) wind_population,

                           sum(trnd_ealt) tornado,
                           sum(trnd_eala) tornado_crop,
                           sum(trnd_ealb) tornado_buildings,
                           sum(trnd_ealpe) tornado_population,

                           sum(tsun_ealt) tsunami,
                           sum(tsun_ealb) tsunami_buildings,
                           sum(tsun_ealpe) tsunami_population,

                           sum(vlcn_ealt) volcano,
                           sum(vlcn_ealb) volcano_buildings,
                           sum(vlcn_ealpe) volcano_population,

                           sum(wfir_ealt) wildfire,
                           sum(wfir_eala) wildfire_crop,
                           sum(wfir_ealb) wildfire_buildings,
                           sum(wfir_ealpe) wildfire_population,

                           sum(wntw_ealt) winterweat,
                           sum(wntw_eala) winterweat_crop,
                           sum(wntw_ealb) winterweat_buildings,
                           sum(wntw_ealpe) winterweat_population
      FROM ${table.table_schema}.${table.table_name}
    `

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const getTableNames = async (pgEnv, src) => {
  const tmpTable = await getTableFromIds(pgEnv, src.source_id, src.view_id);
  return tmpTable.rows[0];
}

const comparativeStatsSwdSummarySql = (dataTables) => `
       SELECT nri_category, 
         max(year) - min(year) num_swd_events_years,
         count(1)::integer num_swd_events_total, 
              sum(
                case
                  when coalesce(property_damage,0) + coalesce(crop_damage,0) + coalesce(injuries_direct,0) + coalesce(injuries_indirect,0) + coalesce(deaths_direct,0) + coalesce(deaths_indirect,0) > 0 then 1
                  else 0
                  end
                )::integer as num_swd_events_loss,
              sum(
                case
                  when coalesce(property_damage,0) + coalesce(crop_damage,0) + coalesce(injuries_direct,0) + coalesce(injuries_indirect,0) + coalesce(deaths_direct,0) + coalesce(deaths_indirect,0) > 0 then 0
                  else 1
                  end
                )::integer as num_swd_events_zero_loss,
  
              coalesce(sum(property_damage), 0)::double precision as swd_events_property_damage, 
              coalesce(sum(crop_damage), 0)::double precision as swd_events_crop_damage,
              coalesce(sum(
                           coalesce(deaths_direct::float,0) +
                           coalesce(deaths_indirect::float,0) +
                           (
                               (
                                   coalesce(injuries_direct::float,0) +
                                   coalesce(injuries_indirect::float,0)
                                 ) / 10
                             )
                         ), 0) * 11600000   as swd_events_person_damage
       FROM ${dataTables.ncei_storm_events_enhanced.table_schema}.${dataTables.ncei_storm_events_enhanced.table_name}
       where (year >= 1996 and year <=2022) and geoid is not null
         and nri_category not in ('OTHER','Marine Dense Fog', 'Dense Fog', 'Astronomical Low Tide','Dust Devil','Dense Smoke','Dust Storm', 'Northern Lights')
       group by 1
           `;
const comparativeStatsPerBasisSummarySql = (dataTables) => `
      SELECT nri_category, count(1)::integer num_per_basis_events_total,
                   sum(
                     case
                       when coalesce(damage,0) > 0 then 1
                       else 0
                       end
                     )::integer as num_per_basis_events_loss,
                   sum(
                     case
                       when coalesce(damage,0) > 0 then 0
                       else 1
                       end
                     )::integer as num_per_basis_events_zero_loss,
                   
                   sum(damage)::double precision as per_basis_total_loss
            FROM ${dataTables.per_basis.table_schema}.${dataTables.per_basis.table_name}
            group by 1
`;
const comparativeStatsNriSummarySql = (dataTables, groupByGeoid = false, geoLevelLen) => `
      SELECT
        ${groupByGeoid ? `substring(stcofips, 1, ${geoLevelLen}) as geoid, ` : ``}
       unnest(array[
           'avalanche', 'coastal', 'coldwave', 
           'drought','earthquake', 'hurricane', 
           'heatwave', 'hail','tornado', 
           'riverine', 'lightning','landslide',
           'icestorm', 'wind', 'wildfire',
           'winterweat', 'tsunami',  'volcano'
            ]) as nri_category,
       
       unnest(array[
           sum(avln_ealt), sum(cfld_ealt), sum(cwav_ealt),
                    
           sum(drgt_ealt), sum(erqk_ealt), sum(hrcn_ealt),
               
           sum(hwav_ealt), sum(hail_ealt), sum(trnd_ealt),
           
           sum(rfld_ealt), sum(ltng_ealt), sum(lnds_ealt),
           
           sum(istm_ealt), sum(swnd_ealt), sum(wfir_ealt),
           
           sum(wntw_ealt), sum(tsun_ealt), sum(vlcn_ealt)
            ]) as nri_loss_total
      
     FROM ${dataTables.nri.table_schema}.${dataTables.nri.table_name}
     ${groupByGeoid ? `group by 1` : ``}
`;
const comparativeStatsHlrSummarySql = (dataTables, groupByGeoid = false, geoLevelLen) => `
      with dollars as (SELECT ${groupByGeoid ? `geoid, ` : ``}
                              nri_category,
                              ctype,
                              CASE
                                WHEN ctype = 'buildings'
                                  THEN sum(CASE
                                             WHEN nri_category IN ('coastal')
                                               THEN hlr * CFLD_EXPB * CFLD_AFREQ
                                             WHEN nri_category IN ('coldwave')
                                               THEN hlr * CWAV_EXPB * CWAV_AFREQ
                                             WHEN nri_category IN ('hurricane')
                                               THEN hlr * HRCN_EXPB * HRCN_AFREQ
                                             WHEN nri_category IN ('heatwave')
                                               THEN hlr * HWAV_EXPB * HWAV_AFREQ
                                             WHEN nri_category IN ('hail')
                                               THEN hlr * HAIL_EXPB * HAIL_AFREQ
                                             WHEN nri_category IN ('tornado')
                                               THEN hlr * TRND_EXPB * TRND_AFREQ
                                             WHEN nri_category IN ('riverine')
                                               THEN hlr * RFLD_EXPB * RFLD_AFREQ
                                             WHEN nri_category IN ('lightning')
                                               THEN hlr * LTNG_EXPB * LTNG_AFREQ
                                             WHEN nri_category IN ('landslide')
                                               THEN hlr * LNDS_EXPB * LNDS_AFREQ
                                             WHEN nri_category IN ('icestorm')
                                               THEN hlr * ISTM_EXPB * ISTM_AFREQ
                                             WHEN nri_category IN ('wind')
                                               THEN hlr * SWND_EXPB * SWND_AFREQ
                                             WHEN nri_category IN ('wildfire')
                                               THEN (
                                                      CASE WHEN geoid IN (select stcofips
                                                                          FROM ${dataTables.nri.table_schema}.${dataTables.nri.table_name}
                                                                          where wfir_hlrb = 0.4)
                                                             THEN 0.4
                                                           ELSE hlr
                                                        END
                                                      ) * WFIR_EXPB * WFIR_AFREQ
                                             WHEN nri_category IN ('winterweat')
                                               THEN hlr * WNTW_EXPB * WNTW_AFREQ
                                             WHEN nri_category IN ('tsunami')
                                               THEN hlr * TSUN_EXPB * TSUN_AFREQ
                                             WHEN nri_category IN ('avalanche')
                                               THEN hlr * AVLN_EXPB * AVLN_AFREQ
                                             WHEN nri_category IN ('volcano')
                                               THEN hlr * VLCN_EXPB * VLCN_AFREQ
                                  END)
                                WHEN ctype = 'crop'
                                  THEN sum(CASE
                                             WHEN nri_category IN ('coldwave')
                                               THEN hlr * CWAV_EXPA * CWAV_AFREQ
                                             WHEN nri_category IN ('drought')
                                               THEN hlr * DRGT_EXPA * DRGT_AFREQ
                                             WHEN nri_category IN ('hurricane')
                                               THEN hlr * HRCN_EXPA * HRCN_AFREQ
                                             WHEN nri_category IN ('heatwave')
                                               THEN hlr * HWAV_EXPA * HWAV_AFREQ
                                             WHEN nri_category IN ('hail')
                                               THEN hlr * HAIL_EXPA * HAIL_AFREQ
                                             WHEN nri_category IN ('tornado')
                                               THEN hlr * TRND_EXPA * TRND_AFREQ
                                             WHEN nri_category IN ('riverine')
                                               THEN hlr * RFLD_EXPA * RFLD_AFREQ
                                             WHEN nri_category IN ('wind')
                                               THEN hlr * SWND_EXPA * SWND_AFREQ
                                             WHEN nri_category IN ('wildfire')
                                               THEN hlr * WFIR_EXPA * WFIR_AFREQ
                                             WHEN nri_category IN ('winterweat')
                                               THEN hlr * WNTW_EXPA * WNTW_AFREQ
                                  END)
                                WHEN ctype = 'population'
                                  THEN sum(CASE
                                             WHEN nri_category IN ('coastal')
                                               THEN hlr * CFLD_EXPPE * CFLD_AFREQ
                                             WHEN nri_category IN ('coldwave')
                                               THEN hlr * CWAV_EXPPE * CWAV_AFREQ
                                             WHEN nri_category IN ('hurricane')
                                               THEN hlr * HRCN_EXPPE * HRCN_AFREQ
                                             WHEN nri_category IN ('heatwave')
                                               THEN hlr * HWAV_EXPPE * HWAV_AFREQ
                                             WHEN nri_category IN ('hail')
                                               THEN hlr * HAIL_EXPPE * HAIL_AFREQ
                                             WHEN nri_category IN ('tornado')
                                               THEN hlr * TRND_EXPPE * TRND_AFREQ
                                             WHEN nri_category IN ('riverine')
                                               THEN hlr * RFLD_EXPPE * RFLD_AFREQ
                                             WHEN nri_category IN ('lightning')
                                               THEN hlr * LTNG_EXPPE * LTNG_AFREQ
                                             WHEN nri_category IN ('landslide')
                                               THEN hlr * LNDS_EXPPE * LNDS_AFREQ
                                             WHEN nri_category IN ('icestorm')
                                               THEN hlr * ISTM_EXPPE * ISTM_AFREQ
                                             WHEN nri_category IN ('wind')
                                               THEN hlr * SWND_EXPPE * SWND_AFREQ
                                             WHEN nri_category IN ('wildfire')
                                               THEN hlr * WFIR_EXPPE * WFIR_AFREQ
                                             WHEN nri_category IN ('winterweat')
                                               THEN hlr * WNTW_EXPPE * WNTW_AFREQ
                                             WHEN nri_category IN ('tsunami')
                                               THEN hlr * TSUN_EXPPE * TSUN_AFREQ
                                             WHEN nri_category IN ('avalanche')
                                               THEN hlr * AVLN_EXPPE * AVLN_AFREQ
                                             WHEN nri_category IN ('volcano')
                                               THEN hlr * VLCN_EXPPE * VLCN_AFREQ
                                  END)
                                END avail
                       FROM ${dataTables.hlr.table_schema}.${dataTables.hlr.table_name}
                  		JOIN ${dataTables.nri.table_schema}.${dataTables.nri.table_name}
                                   ON geoid = stcofips
                       GROUP BY 1, 2 ${groupByGeoid ? `, 3` : ``}

                       ORDER BY 1, 2 ${groupByGeoid ? `, 3` : ``})


      select ${groupByGeoid ? `substring(geoid, 1, ${geoLevelLen}) as geoid, ` : ``} nri_category,
             coalesce(sum(case when ctype = 'buildings' then avail else 0 end))  avail_buildings,
             coalesce(sum(case when ctype = 'crop' then avail else 0 end), 0)      avail_crop,
             coalesce(sum(case when ctype = 'population' then avail else 0 end), 0) avail_population
      from dollars
      group by 1 ${groupByGeoid ? `, 2` : ``}
      order by 1 ${groupByGeoid ? `, 2` : ``}
`;
const comparativeStats = async (pgEnv, source_id, view_id) => {
  const db = await getDb(pgEnv);
  const dependencies = await getViewDependencies(pgEnv, view_id);
  const dataTables = {}

  dataTables['hlr'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'hlr'));
  dataTables['nri'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'nri'));
  dataTables['per_basis'] = await getTableNames(pgEnv, dependencies.find(d => d.type.includes('per_basis')));
  dataTables['ncei_storm_events_enhanced'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'ncei_storm_events_enhanced'));
  dataTables['tl_county'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'tl_county'));
  dataTables['tl_state'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'tl_state'));

  const sql = `
    with 
         swd_summary as (
           ${comparativeStatsSwdSummarySql(dataTables)}
         ),
		 per_basis_summary as (
		    ${comparativeStatsPerBasisSummarySql(dataTables)}
         ),
         nri_summary as (
            ${comparativeStatsNriSummarySql(dataTables)}
         ),

         hlr_summary as (
            ${comparativeStatsHlrSummarySql(dataTables)}
         )

    select a.nri_category,
	-- swd 
	num_swd_events_total, num_swd_events_loss, num_swd_events_zero_loss, swd_events_property_damage, swd_events_crop_damage, swd_events_person_damage,
	swd_events_property_damage + swd_events_crop_damage + swd_events_person_damage as swd_events_total_damage,
	(swd_events_property_damage + swd_events_crop_damage + swd_events_person_damage) / num_swd_events_years as swd_events_total_damage_annualized,
	
	-- per basis
	num_per_basis_events_total, num_per_basis_events_loss, num_per_basis_events_zero_loss, per_basis_total_loss,
	
	-- nri
	nri_loss_total nri_eal,
	
	-- avail
	avail_buildings, avail_crop, avail_population, 
	avail_buildings + avail_crop + avail_population as avail_eal

    from swd_summary as a
           join per_basis_summary as b using(nri_category)
           join nri_summary  as c using(nri_category)
           join hlr_summary as d using(nri_category)
    order by 1 asc
 
  `

  const {
    rows
  } = await db.query(sql);
  return rows;
};


const EALByGeoidByNriCat = async (pgEnv, source_id, view_id, geoids) => {

  const db = await getDb(pgEnv);
  const dependencies = await getViewDependencies(pgEnv, view_id);
  const dataTables = {}

  dataTables['hlr'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'hlr'));
  dataTables['nri'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'nri'));
  const geoLevelGroups = {
    2: geoids.filter(geoid => geoid.toString().length === 2),
    5: geoids.filter(geoid => geoid.toString().length === 5),
    all: geoids.filter(geoid => geoid === 'all')
  }

  const queries =
      Object.keys(geoLevelGroups)
      .filter(geoLevelLen => geoLevelGroups[geoLevelLen].length)
      .map(geoLevelLen => {
        const handleAllGeoReq = geoLevelLen === 'all' ? 5 : geoLevelLen;

        const sql = `
                    with
                      nri_summary as (
                          ${comparativeStatsNriSummarySql(dataTables, true, handleAllGeoReq)}
                      ),
                       hlr_summary as (
                          ${comparativeStatsHlrSummarySql(dataTables, true, handleAllGeoReq)}
                       ),
                       hazard_percents as (
                            select  substring(geoid, 1, ${handleAllGeoReq}) as geoid, 
                                    nri_category, 
                                    PERCENT_RANK() OVER (PARTITION BY nri_category ORDER BY (avail_buildings + avail_crop + avail_population)) national_percent_hazard, 
                                    PERCENT_RANK() OVER (PARTITION BY nri_category, substring (geoid, 1, 2) ORDER BY (avail_buildings + avail_crop + avail_population)) sate_percent_hazard,
                                    -- avail
                                    avail_buildings, avail_crop, avail_population, avail_buildings + avail_crop + avail_population as avail_eal
                      
                            from hlr_summary
                            order by nri_category, 7 desc
                            ),
                      total_percents as (
                            select substring(geoid, 1, ${handleAllGeoReq}) as geoid, sum(avail_buildings + avail_crop + avail_population) avail_eal_total,
                                PERCENT_RANK() OVER (ORDER BY sum (avail_buildings + avail_crop + avail_population)) national_percent_total, 
                                PERCENT_RANK() OVER (PARTITION BY substring (geoid, 1, 2) ORDER BY sum (avail_buildings + avail_crop + avail_population)) state_percent_total
                            from hlr_summary
                            group by geoid
                            order by 3 desc
                            )
                
                    SELECT *, nri_loss_total as nri_eal, 
                           CASE 
                               WHEN COALESCE(nri_loss_total, 0) > 0 AND not COALESCE(avail_eal, 0) > 0 THEN -100
                               WHEN not COALESCE(nri_loss_total, 0) > 0 AND COALESCE(avail_eal, 0) > 0 THEN 100
                               WHEN not COALESCE(nri_loss_total, 0) > 0 AND not COALESCE(avail_eal, 0) > 0 THEN null
                               ELSE ((avail_eal - nri_loss_total) / nri_loss_total) * 100
                            END as diff
                    FROM hazard_percents
                        FULL OUTER JOIN nri_summary
                            USING (geoid, nri_category)
                        JOIN total_percents
                             USING (geoid)
                    ${geoLevelLen === 'all' ? `` : `WHERE substring(geoid, 1, ${geoLevelLen}) IN (${geoLevelGroups[geoLevelLen].map(gid => `'${gid}'`)})`};
                  `;

        return db.query(sql);
      })

  return Promise.all(queries)
      .then(data => Object.keys(queries)
                          .reduce((acc, curr, currI) =>
                              [...acc, ..._.get(data, [currI, 'rows'], [])] , [])
      );
};

const fusionLossByYearByDisasterNumberTotal = async (pgEnv, source_id, view_id) => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
    SELECT 
    EXTRACT(YEAR from coalesce(fema_incident_begin_date, swd_begin_date)) as year,
            coalesce(disaster_number, 'SWD') as disaster_number, 
            sum(fema_property_damage) fema_property_damage, sum(fema_crop_damage) fema_crop_damage, 
            sum(swd_property_damage) swd_property_damage, sum(swd_crop_damage) swd_crop_damage, sum(swd_population_damage) swd_population_damage,
            sum(fusion_property_damage) fusion_property_damage, sum(fusion_crop_damage) fusion_crop_damage 
    FROM ${table.table_schema}.${table.table_name}
    WHERE EXTRACT(YEAR from coalesce(fema_incident_begin_date, swd_begin_date)) is not null
    GROUP BY 1, 2 
    order by 1, 2;
	`;

    const {
      rows
    } = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const fusionLossByYearByDisasterNumberByGeoid = async (pgEnv, source_id, view_id, geoids) => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);
  const geoLevelGroups = {2: geoids.filter(geoid => geoid.toString().length === 2), 5: geoids.filter(geoid => geoid.toString().length === 5)}
  
  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const queries =
        Object.keys(geoLevelGroups)
            .filter(geoLevelLen => geoLevelGroups[geoLevelLen].length)
            .map(geoLevelLen => {
              const sql = `
                          SELECT 
                          substring(geoid, 1, ${geoLevelLen}) as geoid,
                          EXTRACT(YEAR from coalesce(fema_incident_begin_date, swd_begin_date)) as year,
                                  coalesce(disaster_number, 'SWD') as disaster_number, 
                                  count(1) numEvents, 
                                  (ARRAY_AGG(distinct nri_category))[1] as nri_category,
                                  sum(fema_property_damage) fema_property_damage, sum(fema_crop_damage) fema_crop_damage, 
                                  sum(swd_property_damage) swd_property_damage, sum(swd_crop_damage) swd_crop_damage, sum(swd_population_damage) swd_population_damage,
                                  sum(fusion_property_damage) fusion_property_damage, sum(fusion_crop_damage) fusion_crop_damage 
                          FROM ${table.table_schema}.${table.table_name}
                          WHERE EXTRACT(YEAR from coalesce(fema_incident_begin_date, swd_begin_date)) is not null
                          AND substring(geoid, 1, ${geoLevelLen}) IN (${geoids.map(geoid => `'${geoid}'`)})
                          GROUP BY 1, 2, 3 
                          order by 1, 2, 3;
                          `;
              return db.query(sql);
            });

    const rows = await Promise.all(queries)
                              .then(data => Object.keys(queries).reduce((acc, curr, currI) =>
                                  [...acc, ..._.get(data, [currI, 'rows'], [])] , [])
                              );
    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};


const fusionValidateLosses = async (pgEnv, source_id, view_id) => {
  const dependencies = await getViewDependencies(pgEnv, parseInt(view_id));
  const dataTables = {}

  dataTables['ncei_storm_events_enhanced'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'ncei_storm_events_enhanced'));
  dataTables['disaster_loss_summary'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'disaster_loss_summary'));

  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;
    
    const nceiLossesSql = `
                          select sum(property_damage) property_damage, sum(crop_damage) crop_damage, 
                               sum(
                                                                  coalesce(deaths_direct::float,0) +
                                                                  coalesce(deaths_indirect::float,0) +
                                                                  (
                                                                          (
                                                                                  coalesce(injuries_direct::float,0) +
                                                                                  coalesce(injuries_indirect::float,0)
                                                                              ) / 10
                                                                      )
                                                          ) * 11600000 population_damage
                          FROM ${dataTables.ncei_storm_events_enhanced.table_schema}.${dataTables.ncei_storm_events_enhanced.table_name}
                          WHERE year >= 1996 and year <= 2022
                          AND nri_category not in ('Dense Fog', 'Marine Dense Fog', 'Dense Smoke', 'Dust Devil', 'Dust Storm', 'Astronomical Low Tide', 'Northern Lights', 'OTHER')
                          AND geoid is not null;
                            `;
    const {rows: nceiLosses} = await db.query(nceiLossesSql);

    const ofdLossesSql = `
                          SELECT sum(fema_property_damage) property_damage, sum(fema_crop_damage) crop_damage, 0 population_damage
                          FROM ${dataTables.disaster_loss_summary.table_schema}.${dataTables.disaster_loss_summary.table_name};
                            `;
    const {rows: ofdLosses} = await db.query(ofdLossesSql);

    const fusionLossesSql = `
      SELECT  sum(fusion_property_damage) property_damage, sum(fusion_crop_damage) crop_damage,
              sum(fema_property_damage) fema_property_damage, sum(fema_crop_damage) fema_crop_damage,
              sum(swd_property_damage) swd_property_damage, sum(swd_crop_damage) swd_crop_damage, sum(swd_population_damage) swd_population_damage
              FROM ${table.table_schema}.${table.table_name}
    `;

    const {rows: fusionLosses} = await db.query(fusionLossesSql);

    const res = {
      'NCEI': nceiLosses[0],
      'Fusion NCEI': {
        property_damage: fusionLosses[0].swd_property_damage,
        crop_damage: fusionLosses[0].swd_crop_damage,
        population_damage: fusionLosses[0].swd_population_damage,
      },

      'OFD': ofdLosses[0],
      'Fusion OFD': {
        property_damage: fusionLosses[0].fema_property_damage,
        crop_damage: fusionLosses[0].fema_crop_damage,
        population_damage: 0,
      },

      'Fusion': {
        property_damage: fusionLosses[0].property_damage,
        crop_damage: fusionLosses[0].crop_damage,
        population_damage: fusionLosses[0].swd_population_damage,
      }
    }
    acc.push({ ...table, res });

    return acc;
  }, []);

  return res;
};


const fusionDataSourcesBreakdown = async (pgEnv, source_id, view_id) => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const dependencies = await getViewDependencies(pgEnv, parseInt(view_id));
  const dataTables = {}

  dataTables['dds'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'disaster_declarations_summaries_v2'));
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const fusionLossesSql = `
      select a.disaster_number, declaration_title || ' - ' || state as declaration_title,
             sum(fema_property_damage) fema_property_damage, sum(fema_crop_damage) fema_crop_damage,
             coalesce(sum(fema_property_damage), 0) + coalesce(sum(fema_crop_damage), 0) fema_total_damage,
             sum(swd_property_damage)::double precision swd_property_damage, sum(swd_crop_damage)::double precision swd_crop_damage, 
             sum(swd_population_damage)::double precision swd_population_damage,
             coalesce(sum(swd_property_damage), 0) + coalesce(sum(swd_crop_damage), 0) + coalesce(sum(swd_population_damage), 0) swd_total_damage,

            (coalesce(sum(fema_property_damage), 0) + coalesce(sum(fema_crop_damage), 0)) - 
            (coalesce(sum(swd_property_damage), 0) + coalesce(sum(swd_crop_damage), 0) + coalesce(sum(swd_population_damage), 0)) diff

      from ${table.table_schema}.${table.table_name} a
      LEFT JOIN (
      SELECT distinct disaster_number::text, 
          FIRST_VALUE(declaration_title) OVER(PARTITION BY disaster_number) declaration_title,
        FIRST_VALUE(state) OVER(PARTITION BY disaster_number) as state
      FROM ${dataTables.dds.table_schema}.${dataTables.dds.table_name}) b
      ON a.disaster_number = b.disaster_number
      group by 1, 2
    `;
    console.log(fusionLossesSql)
    const {rows} = await db.query(fusionLossesSql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const simpleNRISelect = async (pgEnv, source_id, view_id, id_col='stcofips', id_vals, cols) => {
  const table_names = await getTableFromIds(pgEnv, source_id, view_id);
  const db = await getDb(pgEnv);

  const res = table_names.rows.reduce(async (acc, table) => {
    await acc;

    const sql = `
      SELECT ${id_col} id ${cols.length ? `, ${cols.join(', ')}` : ``}
      from ${table.table_schema}.${table.table_name}
      ${id_vals.length ? `WHERE ${id_col}::text IN (${id_vals.map(idv => `'${idv}'`).join(', ')})` : ``}
    `;

    const {rows} = await db.query(sql);

    acc.push({ ...table, rows });

    return acc;
  }, []);

  return res;
};

const comparativeStatsMega = async (pgEnv, source_id, view_id) => {
  const db = await getDb(pgEnv);
  const dependencies = await getViewDependencies(pgEnv, view_id);
  const dataTables = {}

  dataTables['hlr'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'hlr'));
  dataTables['nri'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'nri'));
  dataTables['per_basis'] = await getTableNames(pgEnv, dependencies.find(d => d.type.includes('per_basis')));
  dataTables['ncei_storm_events_enhanced'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'ncei_storm_events_enhanced'));
  dataTables['tl_county'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'tl_county'));
  dataTables['tl_state'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'tl_state'));

  const pbSql = `
              with pb as (SELECT  geoid, nri_category, ctype, count(1) pb_num_rows, sum(num_events) pb_num_agg_events, sum(damage) pb_damage, sum(damage_adjusted) pb_damage_adjusted
                          FROM ${dataTables.per_basis.table_schema}.${dataTables.per_basis.table_name}
                          group by 1, 2, 3
                         )
              
              select *
              from ${dataTables.hlr.table_schema}.${dataTables.hlr.table_name}
              full join pb
              using(geoid, nri_category, ctype)
  `
  // console.log(pbSql)
  const {
    rows
  } = await db.query(pbSql);
  return rows;
};

module.exports = {
  listPgEnvs,
  numRows,
  eventsByYear,
  eventsByType,
  eventsMappingToNRICategories,
  getTableFromIds,
  per_basis_stats,
  ealFromHlr,
  ealFromEal,
  ddCount, // added new 
  incidentByMonth, // added new
  enhancedNCEILossByYearByType,
  getLtsView,
  nriTotals,
  simpleNRISelect,
  comparativeStats,
  EALByGeoidByNriCat,
  fusionLossByYearByDisasterNumberTotal,
  fusionLossByYearByDisasterNumberByGeoid,
  fusionValidateLosses,
  fusionDataSourcesBreakdown,
  enhancedNCEILossByGeoid,
  comparativeStatsMega,
  enhancedNCEINormalizedCount
};
