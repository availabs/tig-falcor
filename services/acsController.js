const db_service = require("./tig_db"),
  fetch = require("./utils/fetch"),
  get = require("lodash.get");

const BlueBird = require("bluebird");
const { flatten } = require("lodash");

const CENSUS_DATA_API_KEY = require("./utils/censusDataApiKey");

const CensusAcsByGeoidByYearByKey = (geoids, years, censusKeys) => {
  const sql =`
    SELECT
      acs.geoid,
      acs.year,
      acs.censvar,
      acs.value
    FROM census_data.censusdata AS acs
    WHERE acs.geoid = ANY($1)
    AND acs.year = ANY($2)
    AND acs.censvar = ANY($3);
  `
  return db_service.promise(sql, [geoids, years, censusKeys])
    .then(checkRows.bind(null, geoids, years, censusKeys))
}

module.exports = {
  CensusAcsByGeoidByYearByKey
}

const addTo = (object, key, value) => {
  if (!(key in object)) {
    object[key] = value
  }
}

const checkRows = (geoids, years, censusKeys, rows) => {

  if (geoids.length * years.length * censusKeys.length > rows.length) {

    const received = {},
      missingGeoidsAndYears = {},
      missingCensusKeys = {};
    rows.forEach(r => {
      addTo(received, r.geoid, {});
      addTo(received[r.geoid], r.year, {});
      addTo(received[r.geoid][r.year], r.censvar, true);
    })
    geoids.forEach(geoid => {
      years.forEach(year => {
        censusKeys.forEach(censvar => {
          if (!get(received, [geoid, year, censvar], false)) {
            addTo(missingGeoidsAndYears, geoid, {});
            addTo(missingGeoidsAndYears[geoid], year, true);
            addTo(missingCensusKeys, censvar, true);
          }
        })
      })
    })
    return getAcsData(missingGeoidsAndYears, Object.keys(missingCensusKeys))
      .then(data =>
        insertNewData(data)
          .then(() => [].concat(data, rows))
      );
  }
  return rows;
}

let ID = 0;

const SubjectRegex = /^S.+$/;

const getAcsData = (missingGeoidsAndYears, censusKeys) => {
  const [detailKeys, subjectKeys] = censusKeys.reduce((a, c) => {
    if (SubjectRegex.test(c)) {
      a[1].push(c);
    }
    else {
      a[0].push(c);
    }
    return a;
  }, [[], []]);
  const requests = [];
  for (const geoid in missingGeoidsAndYears) {
    for (const year in missingGeoidsAndYears[geoid]) {
// requests are chunked here because you may only supply a maximum of 50 censusKeys per request
      for (let i = 0; i < detailKeys.length; i += 50) {
        const url = makeBaseCensusApiUrl(year, detailKeys.slice(i, i + 50)) + makeUrlFor(geoid, year);
        url && requests.push([geoid, year, url]);
      }
      for (let i = 0; i < subjectKeys.length; i += 50) {
        const url = makeBaseCensusApiUrl(year, subjectKeys.slice(i, i + 50), true) + makeUrlFor(geoid, year);
        url && requests.push([geoid, year, url]);
      }
    }
  }

// requests are chunked here to limit the number of concurrent requests sent to the ACS API at 
  console.log('acs requests', requests)
  return BlueBird.map(requests, ([geoid, year, url]) => {
    return retryFetch(url)
      .then(res => processAcsData(geoid, year, censusKeys.length, res))
  }, { concurrency: 10 }).then(data => flatten(data));

}
const processAcsData = (geoid, year, numKeys, res) => {
  if (!res.length) return [];

  const censusKeys = res[0].slice(0, numKeys),
    data = [];
  for (let i = 1; i < res.length; ++i) {
    data.push(...censusKeys.map((k, ii) => ({ geoid, year, censvar: k, value: +res[i][ii] })));
  }
  return data;
}
const insertNewData = data => {
  if (!data.length) return Promise.resolve();
  const sql = `
    INSERT INTO census_data.censusdata(geoid, year, censvar, value)
    VALUES ${ data.map(d => `('${ d.geoid }', ${ d.year }, '${ d.censvar }', ${ d.value })`) }
  `
  return db_service.promise(sql)
}

const MAX_RETRIES = 5;
const retryFetch = (url, retries = 0) => {
  console.log("ACS RETRY FETCH:", url, retries);
  return new Promise((resolve, reject) => {
    if (retries >= MAX_RETRIES) {
      return resolve([]);
    }
    fetch(url)
      .then(resolve)
      .catch(() => resolve(sleep(500).then(() => retryFetch(url, ++retries))))
  })
}
const sleep = ms =>
  new Promise((resolve, reject) => setTimeout(resolve, ms))

const makeBaseCensusApiUrl = (year, censusKeys, isSubject = false) => {
	return "https://api.census.gov/data/" +
		`${ year }/acs/acs5${ isSubject ? "/subject" : "" }?` +
		`key=${ CENSUS_DATA_API_KEY }` +
		`&get=${ censusKeys }`
}

const getState = geoid => geoid.slice(0, 2),
  getCounty = geoid => geoid.slice(2, 5),
  getPlace = geoid => geoid.slice(2, 7),
  getCosub = geoid => geoid.slice(5, 10),
  getTract = geoid => geoid.slice(5, 11),
  getBlockground = geoid => geoid.slice(11),
  getOther = geoid => geoid.slice(7),
  stateForOther = geoid => geoid.slice(5, 7);

const makeUrlFor = (geoid, year) => {
  if (geoid.startsWith("unsd")) {
    return `&for=school+district+(unified):${ getOther(geoid) }&in=state:${ stateForOther(geoid) }`;
  }
  if ((+year === 2020) && geoid.startsWith("zcta")) {
    return `&for=zip+code+tabulation+area:${ getOther(geoid) }`;
  }
  if ((+year < 2020) && geoid.startsWith("zcta")) {
    return `&for=zip+code+tabulation+area:${ getOther(geoid) }&in=state:${ stateForOther(geoid) }`;
  }
  switch (geoid.length) {
    case 2:
      return `&for=state:${ getState(geoid) }`;
    case 5:
      return `&for=county:${ getCounty(geoid) }&in=state:${ getState(geoid) }`;
    case 7:
      return `&for=place:${ getPlace(geoid) }&in=state:${ getState(geoid) }`;
    case 10:
      return `&for=county+subdivision:${ getCosub(geoid) }&in=state:${ getState(geoid) }+county:${ getCounty(geoid) }`;
    case 11:
      return `&for=tract:${ getTract(geoid) }&in=state:${ getState(geoid) }+county:${ getCounty(geoid) }`;
    case 12:
      return `&for=block+group:${ getBlockground(geoid) }&in=state:${ getState(geoid) }+county:${ getCounty(geoid) }+tract:${ getTract(geoid) }`
  }
}
