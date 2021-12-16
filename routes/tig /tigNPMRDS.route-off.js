const falcorJsonGraph = require("falcor-json-graph");

const TigNPMRDSService = require("../../services/tig/tigNPMRDSController");
const tmcController = require("../../services/tmcController");
const geoController = require("../../services/geoController.npmrds");

const _ = require("lodash");

const $atom = falcorJsonGraph.atom;

const HOURS = _.range(0, 24);

module.exports = [
  {
    route: `tig.npmrds[{keys:monthYears}][{keys:geoms}].data`,
    get: async function(pathSet) {
      const [, , monthYears, geographies] = pathSet;

      const years = _.uniq(
        monthYears.map((monthYear) => monthYear.split("|")[1])
      );

      const tmcsForGeographies = await geoController.getTmcsForGeography(
        geographies,
        years
      );

      const tmcsByGeoByYear = tmcsForGeographies.reduce(
        (acc, { year, geolevel, geoid, tmcs }) => {
          const geo = `${geolevel}|${geoid}`.toUpperCase();

          acc[year] = acc[year] || {};
          acc[year][geo] = tmcs;

          return acc;
        },
        {}
      );

      const tmcsByYear = Object.keys(tmcsByGeoByYear).reduce((acc, year) => {
        const uniqTmcs = new Set();

        Object.keys(tmcsByGeoByYear[year]).forEach((geo) =>
          tmcsByGeoByYear[year][geo].forEach((tmc) => uniqTmcs.add(tmc))
        );

        acc[year] = [...uniqTmcs];

        return acc;
      }, {});

      const seenYears = new Set();
      // Parallel array with resultsData
      const resultsMeta = [];

      const resultsData = await Promise.all(
        _.uniq(monthYears).reduce((acc, yrMo) => {
          const [mo, yr] = yrMo.split("|");

          const year = +yr;
          const month = +mo;

          const tmcs = tmcsByYear[year];

          if (!seenYears.has(year)) {
            acc.push(
              tmcController.tmcMeta(tmcs, [year], ["length", "roadname"])
            );

            resultsMeta.push({ type: "TMC_META", year });

            seenYears.add(year);
          }

          acc.push(TigNPMRDSService.tigNPMRDS(tmcs, +month, +year));

          resultsMeta.push({ type: "AVG_TT", year, month });

          return acc;
        }, [])
      );

      const tmcMetadataByTmcByYear = resultsMeta.reduce(
        (acc, { type, year }, i) => {
          if (type !== "TMC_META") {
            return acc;
          }

          const tmcMetaResult = resultsData[i];

          acc[year] = {};

          for (let i = 0; i < tmcMetaResult.length; ++i) {
            const { tmc, length, roadname } = tmcMetaResult[i];
            acc[year][tmc] = { length, roadname };
          }

          return acc;
        },
        {}
      );

      const avgTTByTmcByMoYr = resultsMeta.reduce(
        (acc, { type, year, month }, i) => {
          if (type !== "AVG_TT") {
            return acc;
          }

          const avgTTResult = resultsData[i];

          const moyr = `${month}|${year}`;

          acc[moyr] = {};

          for (let i = 0; i < avgTTResult.length; ++i) {
            const row = avgTTResult[i];

            acc[moyr][row.tmc] = row;
          }

          return acc;
        },
        {}
      );

      const response = monthYears.reduce((acc, monthYear) => {
        const [mo, yr] = monthYear.split("|");

        const year = +yr;
        const month = +mo;

        const moyr = `${month}|${year}`;

        const tmcsByGeo = tmcsByGeoByYear[year];

        const tmcMetaByTmc = tmcMetadataByTmcByYear[year] || {};
        const avgTTByTmc = avgTTByTmcByMoYr[moyr] || {};

        geographies.forEach((geoKey) => {
          const tmcs = tmcsByGeo[geoKey] || [];

          const data = tmcs.reduce((acc2, tmc) => {
            const { length = null, roadname = null } = tmcMetaByTmc[tmc] || {};
            const avgTTByHr = avgTTByTmc[tmc] || {};

            const s = HOURS.map((hr) => {
              const avgTT = avgTTByHr[`hr_${hr}`];

              const speed =
                avgTT && length ? Math.round(length / (avgTT / 3600)) : null;

              return speed;
            });

            // Truncate null entries from the end of the speeds array
            for (let i = s.length - 1; i >= 0; --i) {
              if (s[i] !== null) {
                break;
              }

              s.length = i;
            }

            // Only include TMCs with speeds data.
            if (s.length) {
              acc2[tmc] = {
                roadname,
                length,
                s,
              };
            }

            return acc2;
          }, {});

          acc.push({
            path: ["tig", "npmrds", monthYear, geoKey, "data"],
            value: $atom(data),
          });
        });

        return acc;
      }, []);

      return response;
    },
  },
];
