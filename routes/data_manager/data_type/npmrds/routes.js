const { atom: $atom, ref: $ref } = require("falcor-json-graph");

const {
  getActiveNpmrdsTravelTimesDamaView,
  getNpmrdsTravelTimesImportViews,
} = require("./controller");

module.exports = [
  {
    route: `dama[{keys:pgEnvs}].npmrds.activeNpmrdsTravelTimesDamaView`,
    get: async function(pathSet) {
      try {
        const { pgEnvs } = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const damaView = await getActiveNpmrdsTravelTimesDamaView(pgEnv);

          result.push({
            path: ["dama", pgEnv, "npmrds", "activeNpmrdsTravelTimesDamaView"],
            value: $atom(damaView),
          });
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },

  {
    route: `dama[{keys:pgEnvs}].npmrds.allNpmrdsTravelTimesImportViews`,
    get: async function(pathSet) {
      try {
        const { pgEnvs } = pathSet;

        const result = [];

        for (const pgEnv of pgEnvs) {
          const damaViews = await getNpmrdsTravelTimesImportViews(pgEnv);

          result.push({
            path: ["dama", pgEnv, "views", "byId", id, "attributes", attr],
            value: typeof v === "object" ? $atom(v) : v, //$atom(value),
          });

          for (const dv of damaViews) {
            const { view_id } = dv;
            result.push({
              path: ["dama", pgEnv, "sources", "byIndex", srcIdx],
              value: $ref(["dama", pgEnv, "sources", "byId", id]),
            });
          }
        }

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    },
  },
];
