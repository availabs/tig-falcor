const dedent = require("dedent");

const { getDb } = require("../../databases");

const DataSourceNames = {
  NpmrdsTravelTimesImp: "NpmrdsTravelTimesImp",
  NpmrdsTravelTimes: "NpmrdsTravelTimes",
};

async function getActiveNpmrdsTravelTimesDamaView(pgEnv) {
  const db = await getDb(pgEnv);

  const sql = dedent(`
    SELECT
        b.*
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE (
        ( a.name = $1 )
        AND
        ( active_end_timestamp IS NULL )
      )
  `);

  const { rows } = await db.query(sql, [DataSourceNames.NpmrdsTravelTimes]);

  if (rows.length === 0) {
    return null;
  }

  if (rows.length > 1) {
    throw new Error(
      "INVARIANT VIOLATION: More than one NpmrdsTravelTimes view with NULL active_end_timestamp."
    );
  }

  return rows[0];
}

async function getNpmrdsTravelTimesImportViews(pgEnv) {
  const db = await getDb(pgEnv);

  const sql = dedent(`
    SELECT
        b.*
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE ( a.name = $1 )
      ORDER BY view_id
  `);

  const { rows } = await db.query(sql, [DataSourceNames.NpmrdsTravelTimesImp]);

  return rows;
}

module.exports = {
  getActiveNpmrdsTravelTimesDamaView,
  getNpmrdsTravelTimesImportViews,
};
