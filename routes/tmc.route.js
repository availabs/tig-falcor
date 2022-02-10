const falcorJsonGraph = require('falcor-json-graph'),
  $atom = falcorJsonGraph.atom,
  $ref = falcorJsonGraph.ref,
  tmcController = require('../services/tmcController');

const { get } = require('lodash');

const tmcMetaColumns = module.exports = [
  'roadnumber',
  'roadname',
  'firstname',
  'tmclinear',
  'country',
  'state_name',
  'county_name',
  'zip',
  'direction',
  'startlat',
  'startlong',
  'endlat',
  'endlong',
  'miles',
  'length', // alias for miles
  'frc',
  'border_set',
  'f_system',
  'faciltype',
  'structype',
  'thrulanes',
  'route_numb',
  'route_sign',
  'route_qual',
  'altrtename',
  'aadt',
  'aadt_singl',
  'aadt_combi',
  'nhs',
  'nhs_pct',
  'strhnt_typ',
  'strhnt_pct',
  'truck',
  'state',
  'is_interstate',
  'is_controlled_access',
  'avg_speedlimit',
  'mpo_code',
  'mpo_acrony',
  'mpo_name',
  'ua_code',
  'ua_name',
  'congestion_level',
  'directionality',
  'bounding_box',
  'avg_vehicle_occupancy',
  'state_code',
  'county_code',
  'type',
  'road_order',
  'isprimary',
  'timezone_name',
  'active_start_date',
  'active_end_date',
  'region_code'
];


const EPOCHS_IN_DAY = 288;
const nullAvgTTArray = [...new Array(EPOCHS_IN_DAY)].fill(null);

module.exports = [
  {
    route: `tmc[{keys:tmcIds}].meta[{keys:years}][${tmcMetaColumns.map(
      c => `"${c}"`
    )}]`,
    get: async function(pathSet) {
      try {
        const pathKeys = pathSet[4];
        const { tmcIds, years } = pathSet;

        const rows = await tmcController.tmcMeta(
          tmcIds,
          years,
          pathKeys
        );
        const dataByYearByTmc = rows.reduce((acc, row) => {
          const { tmc, year } = row;

          acc[tmc] = acc[tmc] || {};
          acc[tmc][year] = acc[tmc][year] = row;

          return acc;
        }, {});

        return tmcIds.reduce((acc, tmc) => {
          for (let i = 0; i < years.length; ++i) {
            const year = years[i];
            const metadata = get(dataByYearByTmc, [tmc, year], null);

            for (let j = 0; j < pathKeys.length; ++j) {
              const pathKey = pathKeys[j];

              if (pathKey === 'bounding_box') {
                acc.push({
                  path: ['tmc', tmc, 'meta', year, pathKey],
                  value: $atom(metadata && metadata[pathKey])
                });
              } else {
                acc.push({
                  path: ['tmc', tmc, 'meta', year, pathKey],
                  value: metadata && metadata[pathKey]
                });
              }
            }
          }
          return acc;
        }, []);
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  },
  { // get geometries for passed TMC ids
    route: `tmc[{keys:tmcIds}].year[{keys:year}].geometries`,
    get: function(pathSet) {
      const toReturn = []
      try {
        const {tmcIds,year} = pathSet;
        return tmcController
            .tmcGeometries(tmcIds,year)
                .then(res => {
                  tmcIds.forEach( tmcId => {
                    const tmpRes = res.rows.filter( f => f.tmc === tmcId)[0];

                    toReturn.push({
                      path: ['tmc', tmcId, 'year', year, 'geometries'],
                      //value : $atom(JSON.parse("{\"geom\": 1}"))
                      value : tmpRes ? $atom(JSON.parse(tmpRes.geom)) : null
                    })
                  })
                  return res
                })
            .then(d =>{
              return toReturn;
            });
      } catch (err) {
        console.error(err);
        throw err;
      }
    }
  }
]