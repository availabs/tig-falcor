const CENSUS_CONFIG = require('../services/utils/censusConfig.js')
const CENSUS_VARIABLES = require('../services/acs_meta/variables.json')

const AcsService = require("../services/acsController"),
    jsonGraph = require('falcor-json-graph'),
    $ref = jsonGraph.ref,
    $error = jsonGraph.error,
    $atom = jsonGraph.atom

const {
	CENSUS_API_VARIABLE_NAMES
} = require("../services/utils/censusApiUtils")

const get = require("lodash.get");

module.exports = [
  { route: `acs[{keys:geoids}][{keys:years}][{keys:censvar}]`,
    get: function(pathSet) {
      const geoids = pathSet.geoids,
        years = pathSet.years.map(d => +d),
        censusKeys = pathSet.censvar;
    return AcsService.CensusAcsByGeoidByYearByKey(geoids, years, censusKeys)
      .then(results => {
        const returnData = [];
        geoids.forEach(geoid => {
          years.forEach(year => {
            censusKeys.forEach(key => {
              const result = results
                .reduce((a, c) => (c.geoid == geoid) && (c.year == year) && (c.censvar == key) ? c : a, null);
                   //console.log('result', result)
               returnData.push({
                 value: result ? result['value'] : -666666666,
                 path: ['acs', geoid, year, key]
               })
             })
          })
        });
        return returnData;
      })
    }
  },

  { route: `acs.meta.[{keys:censvar}].label`,
    get: function(pathSet) {
      return Promise.resolve(
        pathSet[2].map(k => {
          const data = get(CENSUS_VARIABLES, ["variables", k], null),
            path = ["acs", "meta", k, "label"];
          let value = null;
          if (data !== null) {
            if (k.includes("_")) {
              value = data.label
                .replace(/(?:Estimate|Total)!!/g, "")
            }
            else {
               value = data.concept;
            }
          }
          return { path, value };
        })
      )
    }
  }
]
