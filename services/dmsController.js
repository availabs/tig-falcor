const db_service = require("./hazmit_db")

const get = require("lodash.get"),
  d3array = require("d3-array");

const DATA_ATTRIBUTES = [
  "id", "app", "type", "data",
  "created_at", "created_by",
  "updated_at", "updated_by"
];

module.exports = {
  getFormat: appKeys => {
    const sql = `
      SELECT *
      FROM dms.formats
      WHERE app || '+' || type = ANY($1);
    `
    return db_service.promise(sql, [appKeys])
  },

  DATA_ATTRIBUTES,

  dataLength: appKeys => {
    const sql = `
      SELECT app || '+' || type AS key,
        COUNT(1) AS length
      FROM dms.data_items
      WHERE app = $1
      AND type = $2
      GROUP BY 1
    `;
    const promises = appKeys.map(k =>
      db_service.promise(sql, k.split("+"))
    )
    return Promise.all(promises)
      .then(data => [].concat(...data))
  },
  dataByIndex: (appKeys, indices) => {
    const [min, max] = d3array.extent(indices),
      length = (max - min) + 1;

    const sql = `
      SELECT id
      FROM dms.data_items
      WHERE app = $1
      AND type = $2
      LIMIT ${ length }
      OFFSET ${ min }
    `
    const promises = appKeys.map(key =>
      db_service.promise(sql, key.split("+"))
        .then(rows => ({
          key,
          rows: rows.map(({ id }, i) => ({ i: i + min, id }))
        }))
    )
    return Promise.all(promises)
  },
  getDataById: ids => {
    const sql = `
      SELECT id, app, type, data,
        created_at::TEXT, created_by,
        updated_at::TEXT, updated_by
      FROM dms.data_items
      WHERE id = ANY($1)
    `
    return db_service.promise(sql, [ids]);
  },
  setDataById: (id, data, user) => {
    const sql = `
      UPDATE dms.data_items
      SET data = $1,
        updated_at = NOW(),
        updated_by = $2
      WHERE id = $3
      RETURNING id, app, type, data,
        created_at::TEXT, created_by,
        updated_at::TEXT, updated_by;
    `
    return db_service.promise(sql, [data, get(user, "id", null), id]);
  },
  setDataByIdOld: (items, user) =>
    Promise.all(
      Object.keys(items)
        .reduce((a, c) => {
          const item = items[c],
            keys = Object.keys(item).filter(k => DATA_ATTRIBUTES.includes(k)),
            args = keys.map(k => item[k]),
            sql = `
              UPDATE dms.data_items
              SET ${ keys.map((k, i) => `${ k } = $${ i + 1 }`) },
                updated_at = NOW()
                ${ user ? `, updated_by = $${ keys.length + 1 }` : "" }
              WHERE id = $${ keys.length + 1 + (user ? 1 : 0) }
              RETURNING id, app, type, data,
                created_at::TEXT, created_by,
                updated_at::TEXT, updated_by;
            `;

          user && args.push(user.id);
          args.push(c);

          if (keys.length) {
            a.push(db_service.promise(sql, args));
          }
          return a;
        }, [])
    ).then(res => [].concat(...res)),

  createData: (args, user) => {
    const sql = `
      INSERT INTO dms.data_items(app, type, data, created_by, updated_by)
      VALUES ($1, $2, $3, $4, $4)
      returning id, app, type, data,
        created_at::TEXT, created_by,
        updated_at::TEXT, updated_by;
    `
    args.push(get(user, "id", null));

    return db_service.promise(sql, args);
  },

  deleteData: (ids, user) => {
    const sql = `
      DELETE FROM dms.data_items
      WHERE id = ANY($1);
    `
    return db_service.promise(sql, [ids]);
  }
}
