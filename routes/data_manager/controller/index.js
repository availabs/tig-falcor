const format = require("pg-format");
const _ = require("lodash");

const { tig_db } = require("../../../db_service");

const SourceAttributes = [
  "id",
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
  "id",
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
  } = await tig_db.query(sql);

  return num_sources;
};

const getSourceIdsByIndex = async (sourceIndexes) => {
  const sql = `
    SELECT
        id,
        source_idx
      FROM (
        SELECT
            id,
            (ROW_NUMBER() OVER (ORDER BY id) - 1) AS source_idx
          FROM data_manager.sources AS a
      ) AS t
      WHERE ( source_idx = ANY($1::INT[]) )
    ;
  `;

  const { rows } = await tig_db.query(sql, [sourceIndexes]);

  const idsByIdx = rows.reduce((acc, { source_idx, id }) => {
    acc[source_idx] = id;
    return acc;
  }, {});

  return idsByIdx;
};

const getSourceAttributes = async (sourceIds, attributes) => {
  const cols = _(["id", ...attributes])
    .uniq()
    .intersection(SourceAttributes)
    .value();

  const sql = format(
    `
      SELECT
          ${cols.map(() => "%I")}
        FROM data_manager.sources AS a
      WHERE ( id = ANY($1::INT[]) )
    `,
    ...cols
  );

  const { rows } = await tig_db.query(sql, [sourceIds]);

  const attrsById = rows.reduce((acc, row) => {
    const { id } = row;
    acc[id] = row;
    return acc;
  }, {});

  return attrsById;
};

const setSourceAttributes = async (updates) => {
  const id = Object.keys(updates)
  let result = {};
  const sql = `
      UPDATE data_manager.sources SET 
      ${Object.entries(updates[id].attributes)
        .map(([k,v],i) => `${k} = $${i+1}`).join(', ')}
      WHERE id = ANY($${Object.values(updates[id].attributes).length+1})
      RETURNING *
  `;
  
  return tig_db.promise(sql,[...Object.values(updates[id].attributes), id])
}

const getSourceViewIds = async (sourceIds) => {
  const sql = `
    SELECT
        source_id,
        array_agg(id ORDER BY id) AS view_ids
      FROM data_manager.views
      WHERE ( source_id = ANY($1::INT[]) )
      GROUP BY source_id
  `;

  const { rows } = await tig_db.query(sql, [sourceIds]);

  const bySourceIdx = rows.reduce((acc, { source_id, view_ids }) => {
    acc[source_id] = view_ids;
    return acc;
  }, {});

  return bySourceIdx;
};

const getSourceViewsLength = async (sourceIds) => {
  const sql = `
    SELECT
        b.source_id,
        COUNT(b.id) AS num_views
      FROM data_manager.sources AS a
        INNER JOIN data_manager.views AS b
          ON ( a.id = b.source_id )
      WHERE ( source_id = ANY($1::INT[]) )
      GROUP BY b.source_id
  `;

  const { rows } = await tig_db.query(sql, [sourceIds]);

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
            b.source_id,
            (
              ROW_NUMBER()
                OVER (
                  PARTITION BY b.source_id
                  ORDER BY b.id
                )
              - 1
            ) AS view_idx,
            b.id AS view_id
          FROM data_manager.sources AS a
            INNER JOIN data_manager.views AS b
              ON ( a.id = b.source_id )
          WHERE ( source_id = ANY($1::INT[]) )
      ) AS t
      WHERE ( view_idx = ANY($2::INT[]) )
  `;

  const { rows } = await tig_db.query(sql, [sourceIds, viewIndexes]);

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
  const cols = _(["id", ...attributes])
    .uniq()
    .intersection(ViewAttributes)
    .value();

  const sql = format(
    `
      SELECT
          ${cols.map(() => "%I")}
        FROM data_manager.views
      WHERE ( id = ANY($1::INT[]) )
    `,
    ...cols
  );

  const { rows } = await tig_db.query(sql, [viewIds]);

  const attrsById = rows.reduce((acc, row) => {
    const { id } = row;

    acc[id] = row;

    return acc;
  }, {});

  return attrsById;
};

const setViewAttributes = async (updates) => {
  const id = Object.keys(updates)
  let result = {};
  const sql = `
      UPDATE data_manager.views SET 
      ${Object.entries(updates[id].attributes)
        .map(([k,v],i) => `${k} = $${i+1}`).join(', ')}
      WHERE id = ANY($${Object.values(updates[id].attributes).length+1})
      RETURNING *
  `;
  // console.log(sql)
  return tig_db.promise(sql,[...Object.values(updates[id].attributes), id])
}

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
  setViewAttributes
};
