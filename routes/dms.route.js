const falcorJsonGraph = require("falcor-json-graph"),
  $atom = falcorJsonGraph.atom,
  $ref = falcorJsonGraph.ref,
  dmsController = require("../services/dmsController"),
  DATA_ATTRIBUTES = dmsController.DATA_ATTRIBUTES,
  FORMAT_ATTRIUBTES = ["app", "type", "attributes"];

const get = require("lodash.get");

module.exports = [
  {
    route: "dms.format[{keys:appKeys}]['app','type','attributes']",
    get: function(pathSet) {
      const [, , keys] = pathSet;
      return dmsController.getFormat(keys).then((rows) => {
        const response = [];
        keys.forEach((key) => {
          const [app, type] = key.split("+"),
            row = rows.reduce(
              (a, c) => (c.app == app && c.type === type ? c : a),
              {}
            );
          FORMAT_ATTRIUBTES.forEach((att) => {
            const value = row[att] || null;
            response.push({
              path: ["dms", "format", key, att],
              value: att === "attributes" ? $atom(value) : value,
            });
          });
        });
        return response;
      });
    },
  },
  // { route: "dms.format[{keys:apps}].byIndex[{integers:indices}]",
  //   get: function(pathSet) {
  //     return dmsController.formatByIndex(pathSet.apps, pathSet.indices)
  //
  //   }
  // },
  // { route: `dms.format[{keys:apps}][{keys:appKeys}][${ FORMAT_ATTRIBUTES }]`,
  //   get: function(pathSet) {
  //
  //   }
  // },

  {
    route: "dms.data[{keys:appKeys}].length",
    get: function(pathSet) {
      const [, , keys] = pathSet;
      console.log("DMS data get");
      return dmsController.dataLength(keys).then((rows) => {
        console.log("got dms data", keys);
        return keys.map((key) => ({
          path: ["dms", "data", key, "length"],
          value: rows.reduce((a, c) => (c.key === key ? c.length : a), 0),
        }));
      });
    },
  },
  {
    route: "dms.data[{keys:appKeys}].byIndex[{integers:indices}]",
    get: function(pathSet) {
      const [, , keys, , indices] = pathSet;
      console.time("dms data get", keys, indices);
      return dmsController.dataByIndex(keys, indices).then((rows) => {
        console.timeEnd("dms data get", keys, indices);
        const response = [];
        keys.forEach((key) => {
          const reduced = rows.reduce(
            (a, c) => (c.key == key ? c.rows : a),
            []
          );
          indices.forEach((i) => {
            const id = reduced.reduce((a, c) => (c.i == i ? c.id : a), null);
            response.push({
              path: ["dms", "data", key, "byIndex", i],
              value: id ? $ref(["dms", "data", "byId", id]) : null,
            });
          });
        });
        return response;
      });
    },
  },
  {
    route: `dms.data.byId[{keys:ids}]['${DATA_ATTRIBUTES.join("','")}']`,
    get: function(pathSet) {
      const [, , , ids, atts] = pathSet;
      return dmsController
        .getDataById(ids)
        .then((rows) => dataByIdResponse(rows, ids, atts));
    },
    // set: function(jsonGraph) {
    //   const items = get(jsonGraph, ["dms", "data", "byId"], {}),
    //     ids = Object.keys(items);
    //   return dmsController.setDataById(items, this.user)
    //     .then(rows => {
    //      const apps = rows.map(({ app, type }) => ({ app, type}));
    //      return [
    //        ...dataByIdResponse(rows, ids, DATA_ATTRIBUTES),
    //        ...apps.map(({ app, type }) => ({
    //          path: ["dms", "data", `${ app }+${ type }`, "byIndex"],
    //            invalidated: true
    //        }))
    //      ]
    //    });
    // }
  },
  {
    route: "dms.data.edit",
    call: function(callPath, args) {
      const [id, data] = args;
      return dmsController.setDataById(id, data, this.user).then((rows) => {
        // const apps = rows.map(({ app, type }) => ({ app, type}));
        return [
          ...dataByIdResponse(rows, [id], DATA_ATTRIBUTES),
          // ...apps.map(({ app, type }) => ({
          //  path: ["dms", "data", `${ app }+${ type }`, "byIndex"],
          //   invalidated: true
          // }))
        ];
      });
    },
  },
  {
    route: "dms.data.create",
    call: function(callPath, args) {
      const [app, type] = args;
      return dmsController.createData(args, this.user).then((rows) => [
        ...dataByIdResponse(
          rows,
          rows.map(({ id }) => id),
          DATA_ATTRIBUTES
        ),
        { path: ["dms", "data", `${app}+${type}`], invalidated: true },
      ]);
    },
  },
  {
    route: "dms.data.delete",
    call: function(callPath, args) {
      const [app, type, ...ids] = args;
      return dmsController.deleteData(ids, this.user).then((rows) => [
        ...ids.map((id) => ({
          path: ["dms", "data", "byId", id],
          invalidated: true,
        })),
        { path: ["dms", "data", `${app}+${type}`], invalidated: true },
      ]);
    },
  },
];

const dataByIdResponse = (rows, ids, atts) => {
  const response = [];
  ids.forEach((id) => {
    const row = rows.reduce((a, c) => (c.id == id ? c : a), {});
    atts.forEach((att) => {
      const value = row[att] || null;
      response.push({
        path: ["dms", "data", "byId", id, att],
        value: att === "data" ? $atom(value) : value,
      });
    });
  });
  return response;
};
