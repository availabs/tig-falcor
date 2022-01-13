const { uniq } = require("lodash");

const db_service = require("./tig_db");

const tmcMetadataColAliases = {
  length: "miles AS length",
};

const EPOCHS_IN_DAY = 288;

const MIN_META_YEAR = 2016,
  MAX_META_YEAR = 2021;

const tmcMeta = async (tmcArray, years, pathKeys) => {
  const aliasedCols = uniq(pathKeys).map((c) => tmcMetadataColAliases[c] || c);

  const sql = uniq(years)
    .filter((y) => +y)
    .map(
      (year) => `
      SELECT
          tmc,
          ${year} as year,
          ${aliasedCols}
         FROM tmc_metadata_${Math.max(
           MIN_META_YEAR,
           Math.min(MAX_META_YEAR, year)
         )}
        WHERE ( tmc = ANY($1) )`
    ).join(`
    UNION ALL
  `);

  const { rows } = await db_service.query(sql, [uniq(tmcArray)]);

  for (let i = 0; i < rows.length; ++i) {
    const row = rows[i];
    if (row.bounding_box) {
      row.bounding_box = row.bounding_box
        .replace(/.*\(|\).*/g, "")
        .split(",")
        .map((c) => c.split(/\s/).map((x) => +x));
    }
  }

  return rows;
};

const tmcDay = async (tmcArray, dates) => {
  const sql = `
    SELECT
        a.tmc,
        date,
        '{"' || string_agg(cast(a.epoch as varchar) || '":' || cast(a.tt as varchar), ',"') || '}' as tt
      FROM
      (
          SELECT tmc,epoch,date,cast(travel_time_all_vehicles as int) as tt
          FROM npmrds
          WHERE tmc = ANY($1)
          AND date = ANY($2)
      ) as a
      GROUP BY tmc, date
  `;

  // console.log('tmcday\n', sql);
  const { rows } = await db_service.query(sql, [tmcArray, dates]);
  return rows;
};

const tmcAvgDay = async (tmcArray, years) => {
  const sql = `
    SELECT
        tmc,
        year,
        avg_day
      FROM avgtt
      WHERE (
        (tmc = ANY($1))
        AND
        (year = ANY($2))
      )
  `;

  const { rows } = await db_service.query(sql, [tmcArray, years]);

  // Convert the avg_day object to a sparse array.
  for (let i = 0; i < rows.length; ++i) {
    const row = rows[i];
    const { avg_day } = row;

    row.avg_day = new Array(EPOCHS_IN_DAY);

    for (let epoch = 0; epoch < EPOCHS_IN_DAY; ++epoch) {
      const tt = avg_day[epoch];
      row.avg_day[epoch] = tt === undefined ? null : tt;
    }
  }

  return rows;
};

const tmcFullData = async (tmcArray, years) => {
  let queries = years.map((year) => {
    const sql = `
      SELECT
          tmc,
          to_char(date, 'YYYY-MM-DD') || '-' || epoch as dt,
          travel_time_all_vehicles as "tt"
        FROM npmrds
        WHERE (
          (tmc = ANY($1))
          AND
          (
            (date >= '${year}-01-01'::DATE)
            AND
            (date < '${year + 1}-01-01'::DATE)
          )
        )
    `;

    return db_service.query(sql, [tmcArray]);
  });

  const yearlyData = await Promise.all(queries);

  const aggregatedRows = Array.prototype.concat(
    ...yearlyData.map(({ rows }) => rows)
  );

  return aggregatedRows;
};

const npmrds = async (tmcArray, years, dataSources) => {
  const cols = ["tmc", "to_char(date, 'YYYY-MM-DD') || '-' || epoch as ts"];
  if (dataSources.includes("ALL")) {
    cols.push("travel_time_all_vehicles", "data_density_all_vehicles");
  }

  if (dataSources.includes("PASS")) {
    cols.push(
      "COALESCE(travel_time_passenger_vehicles, travel_time_all_vehicles) as travel_time_passenger_vehicles",
      "data_density_passenger_vehicles"
    );
  }

  if (dataSources.includes("TRUCK")) {
    cols.push(
      "COALESCE(travel_time_freight_trucks, travel_time_all_vehicles) AS travel_time_freight_trucks",
      "data_density_freight_trucks"
    );
  }

  const yrs = years.map((y) => +y);

  const sql = `
    SELECT ${cols.join(",")}
      FROM npmrds
      WHERE (
        (tmc = ANY($1))
        AND
        (
          ${yrs
            .map(
              (yr) =>
                `((date >= '${yr}-01-01'::DATE) AND (date < '${yr +
                  1}-01-01'::DATE))`
            )
            .join(" OR ")}
        )
      )
  `;

  // console.time('npmrds query');
  const { rows } = await db_service.query(sql, [tmcArray]);
  // console.timeEnd('npmrds query');

  return rows;
};

const incidentsLength = (tmcIds, years) => {
  const sql = `
    SELECT tmc,
      EXTRACT(year FROM creation) AS year,
      COUNT(1) AS length
    FROM transcom.transcom_events
    WHERE tmc = ANY($1)
    AND EXTRACT(year FROM creation) = ANY($2)
    GROUP BY 1, 2
  `;
  return db_service.promise(sql, [tmcIds, years]);
};
const incidentsByIndex = (tmcIds, years) => {
  const sql = `
    SELECT tmc,
      EXTRACT(year FROM creation) AS year,
      event_id AS eventid
    FROM transcom.transcom_events
    WHERE tmc = ANY($1)
    AND EXTRACT(year FROM creation) = ANY($2)
  `;
  return db_service.promise(sql, [tmcIds, years]);
};

const incidentsByDateLength = (tmcs, dates) => {
  const sql = `
    SELECT tmc,
    creation::DATE::TEXT AS date,
    count(1) AS length
    FROM transcom.transcom_events
    WHERE tmc = ANY($1)
    AND creation::DATE = ANY($2)
    GROUP BY 1, 2
  `;
  return db_service.promise(sql, [tmcs, dates]);
};
const incidentsByDateByIndex = (tmcs, dates) => {
  const sql = `
    SELECT tmc,
      creation::DATE::TEXT AS date,
      event_id AS eventid
    FROM transcom.transcom_events
    WHERE tmc = ANY($1)
    AND creation::DATE = ANY($2)
  `;
  return db_service.promise(sql, [tmcs, dates]);
};

const tmcDayWithFill = (tmcs, dates) => {
  const sql = `
    SELECT npmrds.tmc, epoch, date::TEXT, travel_time_all_vehicles::INT AS tt, avg_day
    FROM npmrds
    INNER JOIN avgtt
    ON EXTRACT(year FROM npmrds.date) = avgtt.year AND npmrds.tmc = avgtt.tmc
    WHERE npmrds.tmc = ANY($1)
    AND date = ANY($2)
  `;
  return db_service.promise(sql, [tmcs, dates]);
};

const tmcDataDateExtents = async (tmcs) => {
  const sql = `
    SELECT
        tmc,
        ARRAY[
          to_char(first_date, 'YYYYMMDD'),
          to_char(last_date, 'YYYYMMDD')
        ] AS date_extent
      FROM public.tmc_date_ranges
      WHERE tmc = ANY($1)
  `;

  const { rows } = await db_service.query(sql, [tmcs]);
  return rows;
};

const npmrdsDataDateExtent = async () => {
  const sql = `
    SELECT
        MIN(first_date) AS min_date,
        MAX(last_date) AS max_date
      from tmc_date_ranges
  `;

  const {
    rows: [{ min_date = null, max_date = null } = {}] = [],
  } = await db_service.query(sql);

  return [min_date, max_date];
};

const tmcGeometries = (tmcIds, year) => {
  //let shape_file = +year >= 2019 ? 'npmrds_extended_shapefile_2019' : 'npmrds_extended_shapefile_2017';
  const sql = `
        --SELECT tmc, st_asgeojson(wkb_geometry) as geom --MultiLineString
        SELECT tmc, st_asgeojson(ST_LineMerge(wkb_geometry)) as geom --lineString
        FROM public.npmrds_shapefile_${year}
        WHERE tmc = ANY($1)`;

  return db_service.query(sql, [tmcIds]);
};
const lengthConflation = () => {
  const sql = `
    SELECT count(1) AS length
    FROM public.conflation_20190828
      `;
  return db_service.promise(sql).then((rows) => rows.pop().length);
};
const tmcByIndex = (matchIds, matchCols) => {
  const sql = `
        SELECT id, ${matchCols}
        FROM public.conflation_20190828
        WHERE ${matchCols} = ANY($1)`;
  // console.log('tmcByIndex sql ', sql, [matchIds])

  return db_service.query(sql, [matchIds]);
};
const tmcGeometriesFromAnyId = (ids) => {
  const sql = `
        SELECT id, osm, npmrds, ris, shst, networklev, st_asgeojson(geom) as geom --MultiLineString
        FROM public.conflation_20190828
        WHERE id = ANY($1)`;

  return db_service.query(sql, [ids]);
};

const tmcIdentification = (type, geoid, year) => {
  // console.log('tmc tmcIdentification got here', type, geoid, year);
  if (!["COUNTY", "STATE", "UA", "MPO", "REGION"].includes(type))
    return Promise.resolve();

  const sql = `
        SELECT tmc, ${type.toLowerCase()}_code
        FROM public.tmc_metadata_${year}
        WHERE ${type.toLowerCase()}_code = ANY($1)`;
  // console.log(sql)
  return db_service.query(sql, [geoid]);
};

module.exports = {
  tmcMeta,
  tmcDay,
  tmcAvgDay,
  tmcFullData,
  npmrds,
  incidentsLength,
  incidentsByIndex,
  incidentsByDateLength,
  incidentsByDateByIndex,
  tmcDayWithFill,
  tmcDataDateExtents,
  npmrdsDataDateExtent,
  tmcGeometries,
  lengthConflation,
  tmcByIndex,
  tmcGeometriesFromAnyId,
  tmcIdentification,

  MIN_META_YEAR,
  MAX_META_YEAR,
};
