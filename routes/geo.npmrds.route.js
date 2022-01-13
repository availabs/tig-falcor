const { get } = require('lodash');
const falcorJsonGraph = require('falcor-json-graph');
const $atom = falcorJsonGraph.atom;
const geoController = require('../services/geoController.npmrds');

const getGeometry = pathSet => {
  const [, geoLevel, geoids] = pathSet;
  return geoController.getGeometry(geoLevel, geoids)
    .then(rows =>{
      // console.log('getGeometry',geoLevel, geoids, rows)
      return geoids.map(geoid => ({
        path: ["geo", geoLevel, geoid, "geometry"],
        value: $atom(rows.reduce((a, c) => c.geoid == geoid ? JSON.parse(c.geom) : a, null))
      }))
    })
}

module.exports = [
  { route: "geo.county[{keys:geoids}].geometry",
    get: getGeometry
  },
  { route: "geo.mpo[{keys:geoids}].geometry",
    get: getGeometry
  },
  { route: "geo.ua[{keys:geoids}].geometry",
    get: getGeometry
  },
  { route: "geo.region[{keys:geoids}].geometry",
    get: getGeometry
  },

  {
    route: `geo[{keys:stateCodes}].geoLevels`,
    get: async function(pathSet) {
      const stateCodes = pathSet.stateCodes.map(stateCode =>
        `0${stateCode}`.slice(-2)
      );

      // Row cols: geolevel, geoid, geoname, bounding_box, states
      const rows = await geoController.getGeoLevels(stateCodes);
      const result = [];
      stateCodes.forEach(stateCode => {
        result.push({
          path: ['geo', stateCode, 'geoLevels'],
          value: $atom(rows.filter(({ states }) => states.includes(stateCode)))
        });
      });
      return result;
    }
  },
  {
    // Example syntheticGeoKey: Albany County => COUNTY|36001
    route: `geoAttributes[{keys:syntheticGeoKeys}][{integers:years}]`,
    get: async function(pathSet) {
      // Filter out malformed synthetic keys
      try {
        const syntheticGeoKeys = pathSet[1];
        const years = pathSet[2];

        const rows = await geoController.getGeoAttributes(
          syntheticGeoKeys,
          years
        );

        const dataByYearByKey = rows.reduce((acc, row) => {
          const { geolevel, geoid, year } = row;

          const k = `${geolevel}|${geoid}`.toUpperCase();

          acc[k] = acc[k] || {};
          acc[k][year] = row;

          return acc;
        }, {});

        const result = syntheticGeoKeys.reduce((acc, k) => {
          for (let i = 0; i < years.length; ++i) {
            const year = years[i];

            const upperCaseK = k.toUpperCase();

            const d = get(dataByYearByKey, [upperCaseK, year], null);

            acc.push({
              path: ['geoAttributes', k, year],
              value: $atom(d)
            });
          }
          return acc;
        }, []);

        return result;
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  },

  {
    // Example syntheticGeoKey: Albany County => COUNTY|36001
    route: `geoTmcs[{keys:syntheticGeoKeys}][{integers:years}]`,
    get: async function(pathSet) {
      try {
        const syntheticGeoKeys = pathSet[1];
        const years = pathSet[2];

        const result = await geoController.getTmcsForGeography(
          syntheticGeoKeys,
          years
        );

        const tmcsByKeyByYear = result.reduce(
          (acc, { geolevel, geoid, year, tmcs }) => {
            const k = `${geolevel}|${geoid}`.toUpperCase();

            acc[k] = acc[k] || {};
            acc[k][year] = tmcs;

            return acc;
          },
          {}
        );

        const response = syntheticGeoKeys.reduce((acc, k) => {
          for (let i = 0; i < years.length; ++i) {
            const year = years[i];

            const upperCaseK = k.toUpperCase();

            const tmcs = get(tmcsByKeyByYear, [upperCaseK, year], null);

            acc.push({
              path: ["geoTmcs", k, year],
              value: $atom(tmcs)
            });
          }
          return acc;
        }, []);

        return response;
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  }
];
