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
                         ), 0) * 7600000   as person_damage
                
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
                         ), 0) * 7600000   as swd_events_person_damage
       FROM ${dataTables.ncei_storm_events_enhanced.table_schema}.${dataTables.ncei_storm_events_enhanced.table_name}
       where (year >= 1996 and year <=2019)
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
const comparativeStatsNriSummarySql = (dataTables) => `
      SELECT 
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
`;
const comparativeStatsHlrSummarySql = (dataTables, groupByGeoid = false) => `
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


      select ${groupByGeoid ? `geoid, ` : ``} nri_category,
             coalesce(sum(case when ctype = 'buildings' then avail else 0 end))  avail_swd_buildings,
             coalesce(sum(case when ctype = 'crop' then avail else 0 end), 0)      avail_swd_crop,
             coalesce(sum(case when ctype = 'population' then avail else 0 end), 0) avail_swd_population
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
  dataTables['per_basis'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'per_basis'));
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
	avail_swd_buildings, avail_swd_crop, avail_swd_population, 
	avail_swd_buildings + avail_swd_crop + avail_swd_population as avail_swd_eal

    from swd_summary as a
           join per_basis_summary as b using(nri_category)
           join nri_summary  as c using(nri_category)
           join hlr_summary as d using(nri_category)
    order by 1 asc
 
  `
  console.log(sql)
  const {
    rows
  } = await db.query(sql);
  return rows;
};


const EALByGeoidByNriCat = async (pgEnv, source_id, view_id) => {
  const db = await getDb(pgEnv);
  const dependencies = await getViewDependencies(pgEnv, view_id);
  const dataTables = {}

  dataTables['hlr'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'hlr'));
  dataTables['nri'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'nri'));
  dataTables['per_basis'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'per_basis'));
  dataTables['ncei_storm_events_enhanced'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'ncei_storm_events_enhanced'));
  dataTables['tl_county'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'tl_county'));
  dataTables['tl_state'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'tl_state'));
  // added new code below
  dataTables['fema_disasters'] = await getTableNames(pgEnv, dependencies.find(d => d.type === 'fema_disasters'));

  const sql = `
    with 
         
         hlr_summary as (
            ${comparativeStatsHlrSummarySql(dataTables, true)}
         )

    select geoid, nri_category,
	
	-- avail
	avail_swd_buildings, avail_swd_crop, avail_swd_population, 
	avail_swd_buildings + avail_swd_crop + avail_swd_population as avail_swd_eal

    from hlr_summary
    order by 2 asc
 
  `;
  const {
    rows
  } = await db.query(sql);
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
  comparativeStats,
  EALByGeoidByNriCat
};
