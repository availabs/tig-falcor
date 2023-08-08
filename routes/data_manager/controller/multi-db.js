const pgFormat = require("pg-format");
const dedent = require("dedent");
const _ = require("lodash");

const { listPgEnvs, getDb } = require("../databases");
const { Graph, alg: GraphAlgorithms } = require("graphlib");

const SourceAttributes = require("../constants/SourceAttributes");
const ViewAttributes = require("../constants/ViewAttributes");

const getSourcesLength = async (pgEnv) => {
  const sql = `
    SELECT
        COUNT(1)::INTEGER AS num_sources
      FROM data_manager.sources
  `;

  const db = await getDb(pgEnv);

  const {
    rows: [{ num_sources }],
  } = await db.query(sql);

  return num_sources;
};

const getDependentsSafetyCheck = async (pgEnv, id, col = "view_id") => {
  const sql =
    col === "view_id"
      ? `SELECT source_id, view_id, view_dependencies, metadata FROM data_manager.views WHERE ${id} = ANY(view_dependencies);`
      : `WITH viewIds AS (SELECT view_id FROM data_manager.views WHERE source_id = ${id})
        SELECT source_id, views.view_id, view_dependencies, metadata FROM data_manager.views JOIN viewIds ON viewIds.view_id = ANY(view_dependencies);`;

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql);

  return rows;
};

const getSourceMetaData = async (pgEnv, id) => {
  const getTablesSql = `
   SELECT source_id, view_id, table_schema, table_name
   FROM data_manager.views
   where source_id IN (${id})
  `;

  const db = await getDb(pgEnv);

  const { rows: table_names } = await db.query(getTablesSql);
  console.log("tables", table_names);
  if (table_names.length) {
    const getMetaSql = `
      SELECT column_name, data_type
--            , character_maximum_length, column_default, is_nullable
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE table_schema = '${table_names[0].table_schema}'
      AND table_name = '${table_names[0].table_name}'
    `;

    const { rows } = await db.query(getMetaSql);
    return rows;
  } else {
    return {};
  }
};

const getSourceIdsByIndex = async (pgEnv, sourceIndexes) => {
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

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [sourceIndexes]);

  const idsByIdx = rows.reduce((acc, { source_idx, source_id }) => {
    acc[source_idx] = source_id;
    return acc;
  }, {});

  return idsByIdx;
};

const getSourceAttributes = async (pgEnv, sourceIds, attributes) => {
  const cols = _(["source_id", ...attributes])
    .uniq()
    .intersection(SourceAttributes)
    .value();

  const sql = pgFormat(
    `
      SELECT
          ${cols.map(() => "%I")}
        FROM data_manager.sources AS a
      WHERE ( source_id = ANY($1::INT[]) )
    `,
    ...cols
  );

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [sourceIds]);

  const attrsById = rows.reduce((acc, row) => {
    const { source_id } = row;
    acc[source_id] = row;
    return acc;
  }, {});

  return attrsById;
};

const setSourceAttributes = async (pgEnv, updates) => {
  const id = Object.keys(updates);

  const sql = `
      UPDATE data_manager.sources SET
      ${Object.entries(updates[id].attributes)
        .map(([k], i) => `${k} = $${i + 1}`)
        .join(", ")}
      WHERE source_id = ANY($${Object.values(updates[id].attributes).length +
        1})
      RETURNING *
  `;

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [
    ...Object.values(updates[id].attributes),
    id,
  ]);

  return rows;
};

const getSourceViewIds = async (pgEnv, sourceIds) => {
  const sql = `
    SELECT
        source_id,
        array_agg(view_id ORDER BY view_id) AS view_ids
      FROM data_manager.views
      WHERE ( source_id = ANY($1::INT[]) )
      GROUP BY source_id
  `;

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [sourceIds]);

  const bySourceIdx = rows.reduce((acc, { source_id, view_ids }) => {
    acc[source_id] = view_ids;
    return acc;
  }, {});

  return bySourceIdx;
};

const getSourceViewsLength = async (pgEnv, sourceIds) => {
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
  // console.log("this gets hit", sql);
  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [sourceIds]);

  const viewsLengthBySourceId = rows.reduce((acc, { source_id, num_views }) => {
    acc[source_id] = num_views;
    return acc;
  }, {});

  return viewsLengthBySourceId;
};

const getViewsIdsByViewIdxBySourceId = async (
  pgEnv,
  sourceIds,
  viewIndexes
) => {
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
                  ORDER BY view_id
                )
              - 1
            ) AS view_idx,
            view_id AS view_id
          FROM data_manager.sources AS a
            INNER JOIN data_manager.views AS b
              USING ( source_id )
          WHERE ( source_id = ANY($1::INT[]) )
      ) AS t
      WHERE ( view_idx = ANY($2::INT[]) )
  `;

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [sourceIds, viewIndexes]);

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

const getViewAttributes = async (pgEnv, viewIds, attributes) => {
  const cols = _(["view_id", ...attributes])
    .uniq()
    .intersection(ViewAttributes)
    .value();

  const sql = pgFormat(
    `
      SELECT
          ${cols.map(() => "%I")}
        FROM data_manager.views
      WHERE ( view_id = ANY($1::INT[]) )
    `,
    ...cols
  );

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [viewIds]);

  const attrsById = rows.reduce((acc, row) => {
    const { view_id } = row;

    acc[view_id] = row;

    return acc;
  }, {});

  return attrsById;
};

const setViewAttributes = async (pgEnv, updates) => {
  const id = Object.keys(updates);

  const sql = `
      UPDATE data_manager.views SET
      ${Object.entries(updates[id].attributes)
        .map(([k], i) => `${k} = $${i + 1}`)
        .join(", ")}
      WHERE view_id = ANY($${Object.values(updates[id].attributes).length + 1})
      RETURNING *
  `;

  const db = await getDb(pgEnv);

  const { rows } = await db.query(sql, [
    ...Object.values(updates[id].attributes),
    id,
  ]);

  return rows;
};

const getDataTableFromViewId = async (db, view_id) => {
  const sql =
      `
       SELECT source_id, view_id, table_schema, table_name
       FROM data_manager.views
       where view_id = ${view_id};
      `;

  const {rows: [{ table_schema, table_name }]} = await db.query(sql);

  return {table_schema, table_name}

}

const getViewMeta = async (db, pgEnv, viewId) => {
  const viewMetaQ = dedent(`
    with get_pkey as
    (SELECT a.attname as int_id_column_name, ${viewId} as view_id
    FROM   pg_index i
    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                         AND a.attnum = ANY(i.indkey)

    WHERE  i.indrelid = (SELECT table_schema || '.' || table_name as name from data_manager.views where view_id = $1)::regclass
    AND    i.indisprimary
    )

    select int_id_column_name, v.table_schema, v.table_name from get_pkey
    join data_manager.views as v on v.view_id = get_pkey.view_id
  `)


  //console.time('getViewMeta')
  let qResult = await db.query(viewMetaQ, [viewId]);
  //console.timeEnd('getViewMeta')
  if (qResult.rows.length !== 1) {
    throw new Error(
      `ERROR: Invalid damaViewId ${viewId}. No such view found in db ${pgEnv}`
    );
  }

  const {
    rows: [{ table_schema, table_name, int_id_column_name }],
  } = qResult;

  if (!int_id_column_name) {
    throw new Error(
      `ERROR: DaMaView ${viewId} does not support integer ID queries.`
    );
  }
  return { table_schema, table_name, int_id_column_name }
}

const queryViewTabledataLength = async (pgEnv, viewId) => {
  const db = await getDb(pgEnv);
  const {
    table_schema,
    table_name,
    int_id_column_name
  } = await getViewMeta(db,pgEnv,viewId)

  const getTableRowCount = pgFormat(
    `SELECT COUNT(1) as num_rows from %I.%I`,
      table_schema,
      table_name
  )

  //console.time('get view table data length query')
  const result = await db.query(getTableRowCount);
  //console.timeEnd('get view table data length query')
  const { rows : [num_rows]} = result
  //console.log('got data', result, result?.rows?.[0]?.num_rows, num_rows)
  return result?.rows?.[0]?.num_rows
}

const queryViewTabledataIndices = async (pgEnv, viewId) => {
  const db = await getDb(pgEnv);
  //console.log('query view table indexes')
  const {
    table_schema,
    table_name,
    int_id_column_name
  } = await getViewMeta(db,pgEnv,viewId)

  const getTableRowCount = pgFormat(
    `SELECT %I as id from %I.%I ORDER BY %I asc`,
      int_id_column_name,
      table_schema,
      table_name,
      int_id_column_name
  )

  //console.time('get view table data indices query')
  const result = await db.query(getTableRowCount);
  //console.timeEnd('get view table data indices query')
  const { rows : [num_rows]} = result
  return result.rows || []
}

const queryViewTabledataById = async (pgEnv, viewId, intIds, attributes) => {
  const db = await getDb(pgEnv);
  console.log('query view table data')
  const {
    table_schema,
    table_name,
    int_id_column_name
  } = await getViewMeta(db,pgEnv,viewId)

  let getWKB = false;
  attributes = [...attributes].filter(a => {
    if (a === "wkb_geometry") {
      getWKB = true;
      return false;
    }
    return true;
  })

  const selectCols = _.uniq([int_id_column_name, ...attributes]);

  const idsList = intIds.map((id) => `(${+id})`);

  const dataQ = dedent(
    pgFormat(
      `
        SELECT ${ selectCols.map(() => "%I").join(", ") }
          ${ getWKB ? ", ST_asGeoJSON(wkb_geometry) AS wkb_geometry" : "" }
          FROM %I.%I
            INNER JOIN (
              VALUES ${idsList}
            ) AS t(%I) USING (%I)
      `,
      ...selectCols,
      table_schema,
      table_name,
      int_id_column_name,
      int_id_column_name
    )
  );
  //console.time('get view table data  query')
  const { rows } = await db.query(dataQ);
  //console.timeEnd('get view table data  query')

  const result = rows.reduce((acc, row) => {
    const id = row[int_id_column_name];

    acc[id] = row;

    return acc;
  }, {});

  return result;
};



const toposortViewDependencyGraph = (rows) => {
  const g = new Graph({
    directed: true,
    multigraph: false,
    compound: false,
  });

  for (const { view_id, view_dependencies } of rows) {
    if (view_dependencies === null) {
      g.setNode(view_id);
      continue;
    }

    // We flatten the depencencies because NpmrdsAuthoritativeTravelTimesDb's is 2-dimensional.
    for (const id of _.flattenDeep(view_dependencies) || []) {
      if (+id !== +view_id) {
        g.setEdge(`${id}`, `${view_id}`);
      }
    }
  }

  const toposortedViewIds = GraphAlgorithms.topsort(g);

  const viewsById = rows.reduce((acc, row) => {
    acc[row.view_id] = row;
    return acc;
  }, {});

  const toposortedViews = toposortedViewIds
    .map((id) => viewsById[id])
    .filter(Boolean);

  return toposortedViews;
};

const getViewDependencies = async (pgEnv, viewId) => {
  const db = await getDb(pgEnv);

  const sql = dedent(`
    WITH RECURSIVE cte_dependencies(source_id, view_id, view_dependencies) AS (
        SELECT
            source_id,
            view_id,
            view_dependencies
          FROM data_manager.views
          WHERE ( view_id = $1 )
        UNION
        SELECT
            a.source_id,
            a.view_id,
            a.view_dependencies
          FROM data_manager.views AS a
            INNER JOIN cte_dependencies AS b
              ON ( a.view_id = ANY(b.view_dependencies) )
    ) SELECT
          source_id,
          view_id,
          type,
          view_dependencies
        FROM cte_dependencies
        JOIN data_manager.sources s
            USING (source_id)
    ;
  `);

  const { rows } = await db.query(sql, [viewId]);

  return toposortViewDependencyGraph(rows);
};

const getViewDependents = async (pgEnv, viewId) => {
  const db = await getDb(pgEnv);

  const sql = dedent(`
    WITH RECURSIVE cte_dependents(source_id, view_id, view_dependencies) AS (
        SELECT
            source_id,
            view_id,
            view_dependencies
          FROM data_manager.views
          WHERE ( view_id = $1 )
        UNION
        SELECT
            a.source_id,
            a.view_id,
            a.view_dependencies
          FROM data_manager.views AS a
            INNER JOIN cte_dependents AS b
              ON ( b.view_id = ANY(a.view_dependencies) )
    ) SELECT
          source_id,
          view_id,
          view_dependencies
        FROM cte_dependents
    ;
  `);

  const { rows } = await db.query(sql, [viewId]);

  return toposortViewDependencyGraph(rows);
};

const getViewDependencySubgraph = async (pgEnv, viewId) => {
  const [dependencies, dependents] = await Promise.all([
    getViewDependencies(pgEnv, viewId),
    getViewDependents(pgEnv, viewId),
  ]);

  // console.log(JSON.stringify({ dependencies, dependents }, null, 4));

  return { dependents, dependencies };
};

async function getEtlContext(pgEnv, etlContextId) {
  const db = await getDb(pgEnv);

  const ctxSql = dedent(`
    SELECT
        *
      FROM data_manager.etl_contexts
      WHERE ( etl_context_id = $1 )
    ;
  `);

  const eventsSql = dedent(`
    SELECT
        *
      FROM data_manager.event_store
      WHERE ( etl_context_id = $1 )
      ORDER BY event_id
    ;
  `);

  const [
    {
      rows: [meta],
    },
    { rows: events },
  ] = await Promise.all([
    db.query(ctxSql, [etlContextId]),
    db.query(eventsSql, [etlContextId]),
  ]);

  return meta ? { meta, events } : null;
}

async function getEtlContextsLatestEventByDamaSourceId(
  pgEnv,
  damaSourceIds,
  etlContextStatuses //: 'OPEN', 'DONE', 'ERROR'
) {
  etlContextStatuses = Array.isArray(etlContextStatuses)
    ? etlContextStatuses
    : [etlContextStatuses];

  damaSourceIds = _.uniq(damaSourceIds.map((id) => +id));

  const db = await getDb(pgEnv);

  const sql = dedent(`
    SELECT
        etl_context_id,

        -- The following two aggregate functions are to avoid a messy GROUP BY clause.
        MIN(source_id) AS source_id,

        (
          array_agg(
            json_build_object(
              'etl_context_id',       a.etl_context_id,
              'parent_context_id',    a.parent_context_id,
              'source_id',            a.source_id,
              'etl_status',           a.etl_status,
              'created_timestamp',    a._created_timestamp,
              'modified_timestamp',   a._modified_timestamp
            )
          )
        )[1] AS etl_context,

        json_object_agg(
          CASE
            WHEN ( a.initial_event_id = b.event_id )
              THEN 'INITIAL'
            ELSE 'LATEST'
          END,

          json_build_object(
            'event_id',   b.event_id,
            'type',       b.type,
            'payload',    b.payload,
            'meta',       b.meta,
            'error',      b.error,
            'timestamp',  b._created_timestamp
          )
        ) AS events

      FROM data_manager.etl_contexts AS a
        INNER JOIN data_manager.event_store AS b
          USING (etl_context_id)

      WHERE (
        ( a.source_id   = ANY($1) )
        AND
        ( a.etl_status  = ANY($2) )
        AND
        ( b.event_id IN (a.initial_event_id, a.latest_event_id) )
      )

      GROUP BY 1
  `);

  const { rows } = await db.query(sql, [damaSourceIds, etlContextStatuses]);

  const etlContextsBySourceId = rows.reduce((acc, context) => {
    const { source_id, etl_context, events } = context;

    acc[source_id] = acc[source_id] || [];
    acc[source_id].push({ etl_context, events });

    return acc;
  }, {});

  return etlContextsBySourceId;
}

async function getSourceIdsByName(pgEnv, damaSrcNames) {
  const db = await getDb(pgEnv);

  const sql = dedent(`
    SELECT
        source_id,
        name
      FROM data_manager.sources
      WHERE ( name = ANY($1) )
    ;
  `);

  const { rows } = await db.query(sql, [damaSrcNames]);

  const idsByName = rows.reduce((acc, { source_id, name }) => {
    acc[name] = source_id;
    return acc;
  }, {});

  return idsByName;
}

const handleFiltersType = (id_col, id_vals, type) => {
  const typeToKeywordMapping = {
    filter: {
      array: 'IN',
      null: 'IS'
    },
    exclude: {
      array: 'NOT IN',
      null: 'IS NOT'
    }
  }

  const arrayVals = id_vals.filter(idv => !['null', 'not null'].includes(idv));
  const nullVals = id_vals.find(idv => ['null', 'not null'].includes(idv));

  return arrayVals.length ? `${id_col}::text ${typeToKeywordMapping[type].array} (${arrayVals.map(idv => `'${idv}'`).join(', ')})` :
      nullVals ? `${id_col} ${typeToKeywordMapping[type].null} ${nullVals}` : ``;

}

const handleFilters = (filters, exclude) => {
  const clauses =
      Object.keys(filters).length || Object.keys(exclude).length ?
          [
            ...Object.keys(filters).length ?
                Object.keys(filters).map((id_col) => handleFiltersType(id_col, filters[id_col], 'filter')) :
                [],
            ...Object.keys(exclude).length ?
                Object.keys(exclude).map((id_col) => handleFiltersType(id_col, exclude[id_col], 'exclude')) :
                []
          ] : [];

  return clauses.length ? `WHERE ${ clauses.join(' and ') }` : ``;
}

const handleGroupBy = (groups) =>
    groups.length ? `GROUP BY ${groups.join(', ')}` : ``;

const handleHaving = (clauses) =>
    clauses.length ? `HAVING ${clauses.map(clause => `(${clause})`).join(' and ')}` : ``;

const handleOrderBy = (orders) =>
    orders.length ? `ORDER BY ${orders.join(', ')}` : ``;

const simpleFilterLength = async (pgEnv, view_id, options) => {
  const db = await getDb(pgEnv);
  const {table_schema, table_name} = await getDataTableFromViewId(db, view_id)

  const {filter = {}, exclude = {}, groupBy = [], having = [], orderBy = [], aggregatedLen = false} = JSON.parse(options);

  const sql = aggregatedLen ?
      `
      with t as (
      SELECT ${groupBy.length ? `${groupBy.join(', ')},` : ``} count(1) numRows
      FROM ${table_schema}.${table_name}
          ${ handleFilters(filter, exclude) }
          ${ handleGroupBy(groupBy) }
          ${ handleHaving(having) }
          )
      SELECT count(1) numRows from t;
    ` :
      `
      SELECT count(1) numRows
      FROM ${table_schema}.${table_name}
          ${ handleFilters(filter, exclude) }
          ${ handleGroupBy(groupBy) }
          ${ handleHaving(having) }
    `;

  const {rows} = await db.query(sql);

  return _.get(rows, [0, 'numrows'], 0);
};

const simpleFilter = async (pgEnv, view_id, options, attributes) => {
  const db = await getDb(pgEnv);
  const {table_schema, table_name} = await getDataTableFromViewId(db, view_id)

  const {filter = {}, exclude = {}, groupBy = [], having = [], orderBy = []} = JSON.parse(options);

  const sql = `
        SELECT ${attributes.join(', ')}
        FROM ${table_schema}.${table_name}
            ${ handleFilters(filter, exclude) }
            ${ handleGroupBy(groupBy) }
            ${ handleHaving(having) }
            ${ handleOrderBy(orderBy) }
            `;

  console.log(sql)
  const {rows} = await db.query(sql);


  return rows;
};

module.exports = {
  listPgEnvs,

  getSourcesLength,
  getSourceIdsByIndex,
  getSourceAttributes,
  setSourceAttributes,
  getSourceViewIds,

  getSourceViewsLength,
  getViewsIdsByViewIdxBySourceId,

  getViewAttributes,
  setViewAttributes,

  getViewMeta,
  queryViewTabledataLength,
  queryViewTabledataIndices,
  queryViewTabledataById,

  getViewDependencies,
  getViewDependents,
  getViewDependencySubgraph,


  getEtlContext,
  getEtlContextsLatestEventByDamaSourceId,

  getDependentsSafetyCheck,

  getSourceMetaData,

  getSourceIdsByName,

  simpleFilterLength,
  simpleFilter
};
