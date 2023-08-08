const { get, chain, mapValues, groupBy } = require("lodash");

const { getDb } = require("../../databases");
const { getViewMeta } = require("../multi-db");

const GEO_LEVEL_MAP = {
  counties: "county",
  cousubs: "cousub",
  state: "state",
  tracts: "tract",
  blockgroup: "bg"
};

async function geoLevelContains(pgEnv, viewIds, geoids, years, geolevels) {
  years = years.map(y => +`${y.toString().slice(0, 3)}0`);
  geolevels = geolevels.map(gl => get(GEO_LEVEL_MAP, gl, gl));

  const db = await getDb(pgEnv);

  const response = {};
  await viewIds.reduce(async (acc, viewId) => {
    await acc;
    const { table_schema, table_name } = await getViewMeta(db, pgEnv, viewId);
    let query = `SELECT name, year, geoid, tiger_type
    FROM ${table_schema}.${table_name}
    WHERE year = ANY($1::INT[])
    AND tiger_type = ANY($2::TEXT[])
    AND geoid LIKE ANY(
      ARRAY(
        SELECT u || '%' 
        FROM UNNEST($3::INT[]) AS t(u)
      )
    )`;
    let { rows } = await db.query(query, [years, geolevels, geoids]);
    rows = chain(rows)
      .groupBy(entry => {
        const matchingGeoid = geoids.find(geoid =>
          entry.geoid.startsWith(geoid)
        );
        return matchingGeoid || [];
      })
      .mapValues(geoidGroup => groupBy(geoidGroup, "year"))
      .mapValues(geoidGroup =>
        mapValues(geoidGroup, yearGroup => groupBy(yearGroup, "tiger_type"))
      )
      .value();
    response[viewId] = rows;
  }, Promise.resolve());

  return response;
}

module.exports = {
  GEO_LEVEL_MAP,

  geoLevelContains
};
