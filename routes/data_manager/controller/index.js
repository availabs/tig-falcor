const pgFormat = require("pg-format");
const dedent = require("dedent");
const _ = require("lodash");

const { npmrds_db } = require("../../../db_service");

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

const getSourcesLength = async () => {
  const sql = `
    SELECT
        COUNT(1)::INTEGER AS num_sources
      FROM data_manager.sources
  `;

  const {
    rows: [{ num_sources }],
  } = await npmrds_db.query(sql);

  return num_sources;
};

const getSourceIdsByIndex = async (sourceIndexes) => {
  const sql = `
    SELECT
        source_id,
        source_idx
      FROM (
        SELECT
            source_id,
            (ROW_NUMBER() OVER (ORDER BY source_id) - 1) AS source_idx
          FROM data_manager.sources AS a
      ) AS t
      WHERE ( source_idx = ANY($1::INT[]) )
    ;
  `;

  const { rows } = await npmrds_db.query(sql, [sourceIndexes]);

  const idsByIdx = rows.reduce((acc, { source_idx, source_id }) => {
    acc[source_idx] = source_id;
    return acc;
  }, {});

  return idsByIdx;
};

const getSourceAttributes = async (sourceIds, attributes) => {
  const cols = _(["source_id", ...attributes])
    .uniq()
    .intersection(SourceAttributes)
    .value();

  const sql = dedent(
    pgFormat(
      `
      SELECT
          ${cols.map(() => "%I")}
        FROM data_manager.sources AS a
      WHERE ( source_id = ANY($1::INT[]) )
    `,
      ...cols
    )
  );

  const { rows } = await npmrds_db.query(sql, [sourceIds]);

  const attrsById = rows.reduce((acc, row) => {
    const { source_id } = row;
    acc[source_id] = row;
    return acc;
  }, {});

  return attrsById;
};

const setSourceAttributes = async (updates) => {
  const id = Object.keys(updates);
  let result = {};

  const sql = dedent(`
      UPDATE data_manager.sources SET 
      ${Object.entries(updates[id].attributes)
        .map(([k, v], i) => `${k} = $${i + 1}`)
        .join(", ")}
      WHERE source_id = ANY($${Object.values(updates[id].attributes).length +
        1})
      RETURNING *
  `);

  return npmrds_db.promise(sql, [...Object.values(updates[id].attributes), id]);
};

const getSourceViewIds = async (sourceIds) => {
  const sql = dedent(`
    SELECT
        source_id,
        array_agg(view_id ORDER BY view_id) AS view_ids
      FROM data_manager.views
      WHERE ( source_id = ANY($1::INT[]) )
      GROUP BY source_id
  `);

  const { rows } = await npmrds_db.query(sql, [sourceIds]);

  const bySourceIdx = rows.reduce((acc, { source_id, view_ids }) => {
    acc[source_id] = view_ids;
    return acc;
  }, {});

  return bySourceIdx;
};

const getSourceViewsLength = async (sourceIds) => {
  const sql = `
    SELECT
        source_id,
        COUNT(view_id) AS num_views
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          USING ( source_id )
      WHERE ( source_id = ANY($1::INT[]) )
      GROUP BY source_id
  `;

  const { rows } = await npmrds_db.query(sql, [sourceIds]);

  const viewsLengthBySourceId = rows.reduce((acc, { source_id, num_views }) => {
    acc[source_id] = num_views;
    return acc;
  }, {});

  return viewsLengthBySourceId;
};

const getViewsIdsByViewIdxBySourceId = async (sourceIds, viewIndexes) => {
  const sql = `
    SELECT
        source_id,
        view_idx,
        view_id
      FROM (
        SELECT
            source_id,
            (
              ROW_NUMBER()
                OVER (
                  PARTITION BY source_id
                  ORDER BY b.view_id
                )
              - 1
            ) AS view_idx,
            view_id
          FROM data_manager.sources AS a
            INNER JOIN data_manager.views AS b
              USING ( source_id )
          WHERE ( source_id = ANY($1::INT[]) )
      ) AS t
      WHERE ( view_idx = ANY($2::INT[]) )
  `;

  const { rows } = await npmrds_db.query(sql, [sourceIds, viewIndexes]);

  const viewsIdsByViewIdxBySourceId = rows.reduce(
    (acc, { source_id, view_idx, view_id }) => {
      acc[source_id] = acc[source_id] || {};

      acc[source_id][view_idx] = view_id;

      return acc;
    },
    {}
  );

  return viewsIdsByViewIdxBySourceId;
};

const getViewAttributes = async (viewIds, attributes) => {
  const cols = _(["view_id", ...attributes])
    .uniq()
    .intersection(ViewAttributes)
    .value();

  const sql = dedent(
    pgFormat(
      `
      SELECT
          ${cols.map(() => "%I")}
        FROM data_manager.views
      WHERE ( view_id = ANY($1::INT[]) )
    `,
      ...cols
    )
  );

  const { rows } = await npmrds_db.query(sql, [viewIds]);

  const attrsById = rows.reduce((acc, row) => {
    const { view_id } = row;

    acc[view_id] = row;

    return acc;
  }, {});

  return attrsById;
};

const setViewAttributes = async (updates) => {
  const id = Object.keys(updates);
  let result = {};

  const sql = dedent(`
      UPDATE data_manager.views SET 
      ${Object.entries(updates[id].attributes)
        .map(([k, v], i) => `${k} = $${i + 1}`)
        .join(", ")}
      WHERE view_id = ANY($${Object.values(updates[id].attributes).length + 1})
      RETURNING *
  `);

  return npmrds_db.promise(sql, [...Object.values(updates[id].attributes), id]);
};

module.exports = {
  SourceAttributes,
  ViewAttributes,

  getSourcesLength,
  getSourceIdsByIndex,
  getSourceAttributes,
  setSourceAttributes,
  getSourceViewIds,

  getSourceViewsLength,
  getViewsIdsByViewIdxBySourceId,

  getViewAttributes,
  setViewAttributes,
};
