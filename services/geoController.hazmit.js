const db_service = require("./tig_db")
const memoizeFs = require('memoize-fs')
const cachePath = require('./cache/cache-path')
const memoizer = memoizeFs({ cachePath })
const get = require('lodash.get')
const {
  EARLIEST_DATA_YEAR,
  LATEST_DATA_YEAR
} = require("./utils/censusApiUtils")
const { getGeoidLengths } = require("./utils/index");
const {
  fillCensusApiUrlArray,
  processCensusApiRow
} = require("./utils/censusApiUtils");

const fetch = require("./utils/fetch");

const typeTables = {
   '2': 'us_state',
   '5': 'us_county',
   '7': '36_place',
   '10': 'cousub',
   '11': 'tract',
   '12' : 'blockgroup',
   'county': 'us_county',
   'tract': 'tract',
   'cousub': 'cousub',
    'blockgroup' : 'blockgroup',
    '0': 'us_state'
}
const getNameColumn = geoLen => geoLen === 7 ? 'name' : 'namelsad';

const TYPE_LENGTHS = {
  state: 2,
  county: 5,
  place: 7,
  cousub: 10,
  tract: 11,
  blockgroup: 12
}

const ZCTAregex = /^zcta-(\d\d)(\d{5})$/,
  UNSDregex = /^unsd-(\d\d)(\d{5})$/;

const ChildrenByGeoid = (geoids, type) => {
  if (type === "zcta") {
    const sql = `
      SELECT zcta_geoid, geoid
      FROM geo.zcta_lookup
      WHERE geoid = ANY($1)
    `
    return db_service.promise(sql, [geoids])
      .then(rows => {
        return geoids.reduce((a, c) => {
          a[c] = rows.filter(({ geoid }) => geoid === c)
            .map(({ zcta_geoid }) => zcta_geoid);
          return a;
        }, {});
      })
  }
  if (type === "unsd") {
    const sql = `
      SELECT unsd_geoid, geoid
      FROM geo.unsd_lookup
      WHERE geoid = ANY($1)
    `
    return db_service.promise(sql, [geoids])
      .then(rows => {
        return geoids.reduce((a, c) => {
          a[c] = rows.filter(({ geoid }) => geoid === c)
            .map(({ unsd_geoid }) => unsd_geoid);
          return a;
        }, {});
      })
  }

  const [cousubs, places, ZCTAs, UNSDs, rest] = geoids.reduce((a, c) => {
    if (((type === "tract") || (type === "blockgroup")) && (c.length === 10)) {
      a[0].push(c);
    }
    else if (((type === "tract") || (type === "blockgroup")) && (c.length === 7)) {
      a[1].push(c);
    }
    else if (c.startsWith("zcta")) {
      a[2].push(c);
    }
    else if (c.startsWith("unsd")) {
      a[3].push(c);
    }
    else {
      a[4].push(c);
    }
    return a;
  }, [[], [], [], [], []]);

  const queries = [];

  if (ZCTAs.length) {
    const sql = `
      SELECT zcta_geoid, geoid
      FROM geo.zcta_lookup
      WHERE zcta_geoid = ANY($1)
      AND char_length(geoid) = $2;
    `
    queries.push(
      db_service.promise(sql, [ZCTAs, TYPE_LENGTHS[type]])
        .then(rows => ZCTAs.reduce((a, c) => {
          a[c] = rows.filter(row => row.zcta_geoid === c).map(({ geoid }) => geoid);
          return a;
        }, {}))
    )
  }
  if (UNSDs.length) {
    const sql = `
      SELECT unsd_geoid, geoid
      FROM geo.unsd_lookup
      WHERE unsd_geoid = ANY($1)
      AND char_length(geoid) = $2;
    `
    queries.push(
      db_service.promise(sql, [UNSDs, TYPE_LENGTHS[type]])
        .then(rows => UNSDs.reduce((a, c) => {
          a[c] = rows.filter(row => row.unsd_geoid === c).map(({ geoid }) => geoid);
          return a;
        }, {}))
    )
  }

  if (cousubs.length) {
    const sql = `
      SELECT cousub_geoid, geoid
      FROM geo.cousub_children
      WHERE cousub_geoid = ANY($1)
      AND char_length(geoid) = $2
    `
    queries.push(
      db_service.promise(sql, [cousubs, TYPE_LENGTHS[type]])
        .then(rows => cousubs.reduce((a, c) => {
          a[c] = rows.reduce((aa, cc) => {
            if (cc.cousub_geoid === c) {
              aa.push(cc.geoid);
            }
            return aa;
          }, []);

          return a;
        }, {}))
    )
  }
  if (places.length) {
    const sql = `
      SELECT place_geoid, geoid
      FROM geo.place_children
      WHERE place_geoid = ANY($1)
      AND char_length(geoid) = $2
    `
    queries.push(
      db_service.promise(sql, [places, TYPE_LENGTHS[type]])
        .then(rows => places.reduce((a, c) => {
          a[c] = rows.reduce((aa, cc) => {
            if (cc.place_geoid === c) {
              aa.push(cc.geoid);
            }
            return aa;
          }, []);

          return a;
        }, {}))
    )
  }
  if (rest.length) {
    const sql = `
      SELECT geoid
      FROM geo.tl_2017_${ typeTables[type] }
      WHERE geoid LIKE ANY($1);
    `
    queries.push(
      db_service.promise(sql, [rest.map(g => g + "%")])
        .then(rows => rest.reduce((a, c) => {
          a[c] = rows.reduce((aa, cc) => {
            if (cc.geoid.slice(0, c.length) === c) {
              aa.push(cc.geoid);
            }
            return aa;
          }, []);

          return a;
        }, {}))
    );
  }
  return Promise.all(queries)
    .then(results =>
      results.reduce((a, c) => {
        for (const k in c) {
          a[k] = c[k];
        }
        return a;
      }, {})
    )
}

const placesOrCousubsByGeoid = (geoids) => {
    const oldsql = `
             with
                places as (
                    SELECT county_geoid, a.geoid, name
                        FROM geo.tl_2017_36_place b
                        JOIN geo.places_in_counties a
                        ON b.geoid = a.geoid
                        WHERE county_geoid =  ANY('{${geoids}}')
                         ),
                cousubs as (
                    SELECT statefp || countyfp as county_geo, geoid, name
                        FROM geo.tl_2017_36_cousub a
                        WHERE statefp || countyfp = ANY('{${geoids}}')
                        AND name not in (SELECT name from places)
                )

            SELECT * FROM cousubs
            UNION
            SELECT * FROM places ORDER BY county_geo, name
    `;

  const queries = getGeoidLengths(geoids).map(geoLen => {
    const sql = `
    with
                cousubs as (
                    SELECT
                     ${
                            geoLen === 2 ? `statefp` : `statefp || countyfp`
                        } as county_geo, geoid, name as original_name,
                    case
                        when namelsad like '%city%' then upper(name || ', City of')
                        else upper(name || ', Town of')
                    end as name, namelsad
                        FROM geo.tl_2017_36_cousub a
                        WHERE ${
                          geoLen === 2 ? `statefp` : `statefp || countyfp`
                        } = ANY('{${geoids}}')
                        ),
		        places as (
                    SELECT
                    ${
                            geoLen === 2
                                ? `substring(county_geoid, 1, ${geoLen})`
                                : `county_geoid`
                        } as county_geoid, a.geoid, name as original_name, upper(name || ', Village of') as name, 'namelsad' as namelsad
                        FROM geo.tl_2017_36_place b
                        JOIN geo.places_in_counties a
                        ON b.geoid = a.geoid
                        WHERE ${
                          geoLen === 2
                            ? `substring(county_geoid, 1, ${geoLen})`
                            : `county_geoid`
                        } =  ANY('{${geoids}}')
                        AND name not in (SELECT original_name from cousubs where namelsad like '%city%')
                         )

            SELECT * FROM cousubs
            UNION
            SELECT * FROM places
            ORDER BY county_geo, name
    `;
    return db_service.promise(sql);
  });
  return Promise.all(queries).then(data => [].concat(...data));
};

const GeoByGeoid = function GeoByGeoid(allGeoids) {
  return new Promise((resolve, reject) => {

    const [geoids, ZCTAs, UNSDs] = allGeoids.reduce((a, c) => {
      if (ZCTAregex.test(c)) {
        a[1].push(c)
      }
      else if (UNSDregex.test(c)) {
        a[2].push(c)
      }
      else {
        a[0].push(c);
      }
      return a;
    }, [[], [], []])

    // split geography type by geoid length
    let geoidLengths = geoids.reduce((out, g) => {
      if (!out.includes(g.length)) { out.push(g.length) }
      return out
    }, [])

    // create query promises for all geography types
    let queries = geoidLengths.map(geoLen => {

        return new Promise((resolve, reject) => {
        let filteredGeoids = geoids.filter(d => d.length === geoLen)
        // console.log('geoids', geoids,filteredGeoids)

        const sql = `
          SELECT geoid,
            ${ getNameColumn(geoLen) } as name
          FROM geo.tl_2017_${typeTables[geoLen]}
            where geoid in ('${filteredGeoids.join(`','`)}')
        `
        // sql query for debugging
// console.log(sql)

        // run query resolve rows
        db_service.query(sql, [], (err, data) => {
          if (err) reject(err);
          resolve(data ? data.rows : [])
        });
      })
    })

    if (ZCTAs.length) {
      const sql = `
        SELECT geoid, stusps AS name
        FROM geo.tl_2017_us_state
        WHERE geoid = ANY($1)
      `
      const states = [...new Set(ZCTAs.map(zcta => zcta.slice(5, 7)))];
      queries.push(
        db_service.promise(sql, [states])
          .then(rows => rows.reduce((a, c) => {
            a[c.geoid] = c.name;
            return a;
          }, {}))
          .then(map => ZCTAs.map(zcta => {
            const state = ZCTAregex.exec(zcta)[1],
              stateName = map[state];
            return ({
              geoid: zcta,
              name: `${ stateName } ZCTA ${ zcta.slice(7) }`
            })
          }))
      )
    }
    if (UNSDs.length) {
      const sql = `
        SELECT 'unsd-' || geoid20 AS geoid,
          name20 AS name,
          lograde20 || ' - ' || higrade20 AS grades
        FROM geo.tl_2020_36_unsd20
        WHERE geoid20 = ANY($1)
      `
      queries.push(
        db_service.promise(sql, [UNSDs.map(unsd => unsd.slice(5))])
      )
    }

    //run all queries and resolve flattened 1-dimensional array
    Promise.all(queries).then(riskData => {
      resolve([].concat(...riskData))
    })
  });
};

const fipsNameByFips = function fipsNameByFips(geoids){
    return new Promise((resolve, reject) => {
        // split geography type by geoid length
        let geoidLengths = geoids.reduce((out, g) => {
            if (!out.includes(g.length)) { out.push(g.length) }
            return out
        }, [])

        let queries = geoidLengths.map(geoLen => {

            return new Promise((resolve, reject) => {
                let filteredGeoids = geoids.filter(d => d.length === geoLen)
                // console.log('geoids', geoids,filteredGeoids)

                const sql = `
          SELECT
            geoid,
            ${ getNameColumn(geoLen) } as name,
            stusps as state_abbr
          FROM geo.tl_2017_${typeTables[geoLen]}
            where geoid in ('${filteredGeoids.join(`','`)}')
        `;
                // sql query for debugging
// console.log(sql)

                // run query resolve rows
                db_service.query(sql, [], (err, data) => {
                    if (err) reject(err);
                    resolve(data ? data.rows : [])
                });
            })
        })
        Promise.all(queries).then(riskData => {
            resolve([].concat(...riskData))
        })
    })
}

const ZipCodesByGeoid = function ZipCodesByGeoid(geoids){
    return new Promise((resolve,reject) =>{

        let geoidLengths = geoids.reduce((out, g) => {
            if (!out.includes(g.length)) { out.push(g.length) }
            return out
        }, [])

        // create query promises for all geography types
        let queries = geoidLengths.map(geoLen => {

            return new Promise((resolve, reject) => {
                let filteredGeoids = geoids.filter(d => d.length === geoLen)
                // console.log('geoids', geoids,filteredGeoids)

            const sql = `
            SELECT
            zcta5ce10 as zip_codes,
            b.geoid,
            ${ getNameColumn(geoLen) } as name
            FROM geo.cb_2017_us_zcta510 as a
            JOIN geo.tl_2017_${typeTables[geoLen]} as b on ST_INTERSECTS(b.geom,a.geom)
            where geoid in ('${filteredGeoids.join(`','`)}')
        `
                // sql query for debugging
// console.log(sql)

                // run query resolve rows
                db_service.query(sql, [], (err, data) => {
                    if (err) reject(err);
                    resolve(data ? data.rows : [])
                });
            })
        })

        Promise.all(queries).then(riskData => {
            resolve([].concat(...riskData))
        })
    })
};

const _CensusAcsByGeoidByYear = (geoids, years) => {
  const urls = fillCensusApiUrlArray(geoids, years);
  return Promise.all(generateCensusAcsByGeoidByYearFetches(urls))
    .then(data => [].concat(...data));
}


/*
const  CensusAcsByGeoidByYearByKey = (db_service, geoids, years, censusKeys) => {
    const urls = fillCensusApiUrlArray(geoids, years, censusKeys);
    return Promise.all(generateCensusAcsByGeoidByKeyFetches(urls))
            .then(data =>
        [].concat(...data));
}
 */

// const  CensusAcsByGeoidByYearByKey = (geoids, years, censusKeys) => {
//     const queries = years.map(year => {
//         const sql =
//              `SELECT
//                 acs.geoid,
//                 acs.year,
//                 acs.censvar,
//                 acs.value
//
//               FROM census_data.censusdata AS acs
//               WHERE acs.geoid in ('${ geoids.join("','") }')
//               AND acs.censvar in ('${ censusKeys.join("','") }')
//               AND acs.year = ${ Math.min(Math.max(EARLIEST_DATA_YEAR, year), LATEST_DATA_YEAR) }
//             `
//         return db_service.promise(sql);
//     })
//
//     return Promise.all(queries)
//         .then(data => [].concat(...data)
//         );
//
// }

const CensusAcsByGeoidByYear = (geoids, years) => {
    // (console.log('testing years', years, EARLIEST_DATA_YEAR, LATEST_DATA_YEAR))
  const queries = years.map(year => {
    const sql = `
      WITH minus_5 AS (
        SELECT geoid,
        population,
        poverty,
        non_english_speaking,
        under_5,
        over_64,
        non_english_speaking + under_5 + over_64 AS vulnerable
        FROM public.acs_data
        WHERE geoid in ('${ geoids.join("','") }')
        AND year = ${ Math.min(Math.max(EARLIEST_DATA_YEAR, year - 5), LATEST_DATA_YEAR) }
      )

      SELECT acs.geoid,
        acs.year,
        acs.population,
        acs.poverty,
        acs.non_english_speaking,
        acs.under_5,
        acs.over_64,
        acs.non_english_speaking + acs.under_5 + acs.over_64 AS vulnerable,

        acs.population - minus_5.population AS population_change,
        acs.poverty - minus_5.poverty AS poverty_change,
        acs.non_english_speaking - minus_5.non_english_speaking AS non_english_speaking_change,
        acs.under_5 - minus_5.under_5 AS under_5_change,
        acs.over_64 - minus_5.over_64 AS over_64_change,
        acs.non_english_speaking + acs.under_5 + acs.over_64 - minus_5.vulnerable AS vulnerable_change

      FROM public.acs_data AS acs
      JOIN minus_5
      ON acs.geoid = minus_5.geoid
      WHERE acs.geoid in ('${ geoids.join("','") }')
      AND acs.year = ${ Math.min(Math.max(EARLIEST_DATA_YEAR, year), LATEST_DATA_YEAR) }
    `
    return db_service.promise(sql);
  })
  return Promise.all(queries)
    .then(data => [].concat(...data));
}

const getBlockgroupCentroid = geoids => {
  const sql = `
    SELECT ST_ASGEOJSON(ST_CENTROID(ST_EXTENT(geom))) AS geojson
    FROM geo.tl_2017_blockgroup
    WHERE geoid = ANY($1);
  `
  return db_service.promise(sql, [geoids])
    .then(res => res.length ? JSON.parse(res[0].geojson) : null)
}
const getBoundingBoxByGeoid = allGeoids =>{

    const [geoids, ZCTAs, UNSDs] = allGeoids.reduce((a, c) => {
      if (ZCTAregex.test(c)) {
        a[1].push(c)
      }
      else if (UNSDregex.test(c)) {
        a[2].push(c)
      }
      else {
        a[0].push(c);
      }
      return a;
    }, [[], [], []]);

    return new Promise((resolve, reject) => {
        let geoidLengths = geoids.reduce((out, g) => {
            if (!out.includes(g.length)) {
                out.push(g.length)
            }
            return out
        }, [])
        let queries = geoidLengths.map(geoLen => {

            return new Promise((resolve, reject) => {
                let filteredGeoids = geoids.filter(d => d.length === geoLen)
                const sql = `
                  SELECT
                    geoid,
                    ST_Extent(geom) as bounding_box,
                    st_asgeojson(geom) as geom
                  FROM geo.tl_2017_${typeTables[geoLen]}
                    where geoid in ('${filteredGeoids.join(`','`)}')
                    GROUP BY geoid,geom
            `
                db_service.query(sql, [], (err, data) => {
                    if (err) reject(err);
                    resolve(data ? data.rows : [])
                })
            })
        })

        if (ZCTAs.length) {
          const sql = `
            SELECT 'zcta-36' || zcta5ce10 AS geoid,
              ST_AsGeoJSON(geom) AS geom,
              ST_Extent(geom) as bounding_box
            FROM geo.tl_2017_us_zcta510
            WHERE 'zcta-36' || zcta5ce10 = ANY($1)
            GROUP BY 1, 2;
          `
          queries.push(db_service.promise(sql, [ZCTAs]));
        }
        if (UNSDs.length) {
          const sql = `
            SELECT 'unsd-' || geoid20 AS geoid,
              ST_AsGeoJSON(geom) AS geom,
              ST_Extent(geom) as bounding_box
            FROM geo.tl_2020_36_unsd20
            WHERE 'unsd-' || geoid20 = ANY($1)
            GROUP BY 1, 2;
          `
          queries.push(db_service.promise(sql, [UNSDs]));
        }

        Promise.all(queries).then(data => {
            resolve([].concat(...data))
        })
    })

}

const GEO_LEVEL_MAP = {
  'counties': 'county',
  'cousubs': 'cousub',
  'tracts': 'tract',
  'blockgroup': 'bg'
}

const getIntersections = (geoids, years, geolevels) => {
  years = years.map(y => +`${ y.toString().slice(0, 3) }0`);
  geolevels = geolevels.map(gl => get(GEO_LEVEL_MAP, gl, gl));

  const sql = `
    SELECT geoid2 AS intersection, year, geoid1 AS geoid, type2 AS geolevel
    FROM geo.geo_lookup
    WHERE geoid1 = ANY($1)
    AND year = ANY($2)
    AND type2 = ANY($3)

    UNION

    SELECT geoid1 AS intersection, year, geoid2 AS geoid, type1 AS geolevel
    FROM geo.geo_lookup
    WHERE geoid2 = ANY($1)
    AND year = ANY($2)
    AND type1 = ANY($3)
  `
  return db_service.promise(sql, [geoids, years, geolevels])
}

module.exports = {
  GeoByGeoid,
  fipsNameByFips,
  CensusAcsByGeoidByYear,
  ChildrenByGeoid,
  getBlockgroupCentroid,
  getBoundingBoxByGeoid,
  placesOrCousubsByGeoid,
  ZipCodesByGeoid,

  getIntersections,
  GEO_LEVEL_MAP,

  ZipCodesByGeoidMem: memoizer.fn(ZipCodesByGeoid)
}

const generateCensusAcsByGeoidByYearFetches = urls => {
    return urls.map(([year, url]) =>
        fetch(url)
            .then(data => {
                // console.log(data)
                return data.slice(1)// ignore description row
                .map(d => {
                    return processCensusApiRow(d, year)
                })
            })
    )

}

const generateCensusAcsByGeoidByKeyFetches = urls => {
    return urls.map(([year, url]) =>
        fetch(url)
            .then(data => {
                return data.slice(1)// ignore description row
            .map(d => {
                if (d[d.length-1].length >= 5){ //for cousub,tract
                    let arrSlice = d.slice(1).slice(-3);
                    let county = arrSlice[0]+arrSlice[1]+arrSlice[2]
                    return processCensusApiRow(d,county,year, censusKeys)
                }
                else if (d[d.length-1].length === 1){ // for blockgroup
                    let arrSlice = d.slice(1).slice(-4);
                    let county = arrSlice[0]+arrSlice[1]+arrSlice[2]+arrSlice[3]
                    return processCensusApiRow(d,county,year, censusKeys)
                }
                else if (d[d.length-1].length === 3){ // for county
                    let arrSlice = d.slice(1).slice(-2);
                    let county = arrSlice[0]+arrSlice[1]
                    return processCensusApiRow(d,county,year, censusKeys)
                }
                else if(d[d.length-1].length === 2){ // for state
                    let arrSlice = d.slice(1).slice(-1);
                    let county = arrSlice[0]
                    return processCensusApiRow(d,county,year, censusKeys)
                }

    })
})
)

}
