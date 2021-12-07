const { uniq } = require('lodash');

const db_service = require("./npmrds_db")

const codeColMappings = {
  STATE: "state_code",
  COUNTY: "county_code",
  MPO: "mpo_code",
  UA: "ua_code",
  REGION: "region_code"
};

const getGeoCodesByGeoLevel = syntheticGeoKeys => {
  const keys = _.uniq(syntheticGeoKeys).filter(k => {
    const isValid = k.split("|").length === 2;
    if (!isValid) {
      console.error(
        `WARNING: Disregarding malformed geo_attrs syntheticGeoKey: ${k}`
      );
    }

    return isValid;
  });

  return keys.reduce((acc, k) => {
    const [geolevel, geocode] = k.toUpperCase().split("|");
    acc[geolevel] = acc[geolevel] || [];
    acc[geolevel].push(geocode);
    return acc;
  }, {});
};

const parseBoundingBox = bounding_box =>
  bounding_box &&
  bounding_box
    .replace(/POLYGON\(\(|\)\)/g, '')
    .split(',')
    .filter((e, i) => i === 0 || i === 2)
    .map(corner => corner.split(' ').map(coord => +coord));

const getGeoLevels = async (stateCodes) => {
  const sql = `
    SELECT
      geography_level AS geolevel,
      geography_level_code AS geoid,
      geography_level_name AS geoname,
      ST_AsText(bounding_box) AS bounding_box,
      state_codes AS states
    FROM geography_metadata
    WHERE ( state_codes && $1 )
  `;
  // console.log('geography_metadata', sql, stateCodes )

  const { rows } = await db_service.query(sql, [[stateCodes]]);

  return rows.map(row =>
    Object.assign(row, {
      bounding_box: parseBoundingBox(row.bounding_box)
    })
  );
};

const getGeoAttributes = async (syntheticGeoKeys, years) => {
  const geocodesByGeolevel = getGeoCodesByGeoLevel(syntheticGeoKeys);

  const parameters = [];

  const sql = Object.keys(geocodesByGeolevel).reduce((acc, geolevel) => {
    const geocodeCol = codeColMappings[geolevel];
    const geonameCol = geocodeCol.replace(/code$/, 'name');

    const geocodes = uniq(geocodesByGeolevel[geolevel]);

    const whereClauses = geocodes.map(geocode => {
      parameters.push(geocode);
      return `(${geocodeCol} = $${parameters.length})`;
    });

    for (let i = 0; i < years.length; ++i) {
      const year = years[i];

      acc.push(`
        SELECT
            '${geolevel}' AS geolevel,
            ${geocodeCol} AS geoid,
            ${geonameCol} AS geoname,
            ROUND(
              SUM(
                m.miles::NUMERIC
                * m.is_interstate::INT
                * (nhs_pct::NUMERIC / 100::NUMERIC)
              )
              , 2
            )::DOUBLE PRECISION AS interstate_miles,
            SUM((m.is_interstate AND nhs_pct > 0)::INT)::INT AS interstate_tmcs_ct,
            ROUND(
              SUM(
                m.miles::NUMERIC
                * (NOT m.is_interstate)::INT
                * (nhs_pct::NUMERIC / 100::NUMERIC)
              )
              , 2
            )::DOUBLE PRECISION AS noninterstate_miles,
            SUM(((NOT m.is_interstate) AND (nhs_pct > 0))::INT)::INT AS noninterstate_tmcs_ct,
            ST_AsText(
              ST_Extent(m.bounding_box)
            ) AS bounding_box,
            NULL AS population_info,
            JSON_AGG(DISTINCT m.state_code ORDER BY m.state_code) AS states,
            ${year} AS year
          FROM tmc_metadata_${year} AS m
            ${geolevel === 'REGION' ? `LEFT OUTER JOIN (
              SELECT
                  name AS region_name,
                  fips_code AS county_code
                FROM ny.nysdot_regions AS x
                  INNER JOIN ny.nysdot_region_names AS y
                    USING (region)
            ) AS r
              USING ( county_code )` : ''}
          WHERE ( ${whereClauses.join(' OR ')} )
          GROUP BY ${geocodeCol}, ${geonameCol}`);
    }

    return acc;
  }, []).join(`
    UNION ALL
  `);

  // console.log('geoattributes', sql, parameters)

  const { rows } = await db_service.query(sql, parameters);

  return rows.map(row =>
    Object.assign(row, {
      bounding_box: parseBoundingBox(row.bounding_box)
    })
  );
};

const getTmcsForGeography = async (syntheticGeoKeys, years) => {
  const geocodesByGeolevel = getGeoCodesByGeoLevel(syntheticGeoKeys);

  const parameters = [];

  const sql = Object.keys(geocodesByGeolevel).reduce((acc, geolevel) => {
    const geocodeCol = codeColMappings[geolevel];

    const geocodes = _.uniq(geocodesByGeolevel[geolevel]);

    const whereClauses = geocodes.map(geocode => {
      parameters.push(geocode);
      return `(${geocodeCol} = $${parameters.length})`;
    });

    for (let i = 0; i < years.length; ++i) {
      const year = +years[i];

      acc.push(`
        SELECT
            ${year} AS year,
            '${geolevel}' AS geolevel,
            ${geocodeCol} AS geoid,
            array_agg(distinct tmc) AS tmcs
          FROM tmc_metadata_${year}
          WHERE ( ${whereClauses.join(" OR ")} )
          GROUP BY ${geocodeCol}`);
    }

    return acc;
  }, []).join(`
    UNION ALL
  `);

  const { rows } = await db_service.query(sql, parameters);

  return rows;
};

const GEO_ID = {
  county: "geoid",
  mpo: "mpo_id",
  ua: "geoid10",
  region: 'region'
}
const GEOM_COLUMNS = {
  county: "geom",
  mpo: "wkb_geometry",
  ua: "wkb_geometry",
  region: "geom"
}
const GEOM_TABLES = {
  county: "tl_2017_us_county",
  mpo: "mpo_boundaries",
  ua: "urban_area_boundaries",
  region: "ny.nysdot_region_boundaries"
}

module.exports = {
  getGeoLevels,
  getGeoAttributes,

  getGeometry: (geoLevel, geoids) => {
    const sql = `
      SELECT ${ GEO_ID[geoLevel] } AS geoid,
        ST_ASGEOJSON(${ GEOM_COLUMNS[geoLevel] }) AS geom
      FROM ${ GEOM_TABLES[geoLevel] }
      WHERE ${ GEO_ID[geoLevel] } = ANY($1)
    `
    return db_service.promise(sql, [geoids]);
  },

  getTmcsForGeography
};
