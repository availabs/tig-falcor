const CENSUS_CONFIG = require('../services/utils/censusConfig.js')

const GeoService = require("../services/geoController.hazmit"),
    jsonGraph = require('falcor-json-graph'),
    $ref = jsonGraph.ref,
    $error = jsonGraph.error,
    $atom = jsonGraph.atom;

const GL_MAP = GeoService.GEO_LEVEL_MAP;

const get = require("lodash.get")

const {
	CENSUS_API_VARIABLE_NAMES
} = require("../services/utils/censusApiUtils")

const typeByGeoidLength =  {
	'2': 'state',
	'5': 'county',
	'7': 'place',
	'10': 'cousub',
	'11': 'tract',
	'12': 'blockgroup'
}

const regex = /unsd|zcta/
const getType = row => {
  if (regex.test(row.geoid)) {
    return row.slice(0, 4);
  }
  return typeByGeoidLength[row.geoid.length]
}

module.exports = [//{
	{ // GeoByGeoid
		route: `geo[{keys:geoids}]['geoid', 'name', 'type', 'grades']`,
	    get: function (pathSet) {
	    	let response = [];
	    	var pathKeys = pathSet[2]; // why? look into this
	    	return new Promise((resolve, reject) => {
	    		// force types
	    		let geoids = pathSet.geoids.map(d => d.toString()) // for keys to string
	    		GeoService.GeoByGeoid(geoids).then(geodata => {
    				// console.log('sheldusData', sheldusData.length)
    				geodata.forEach(row => {
    					pathKeys.forEach(key => {
	    					response.push({
	    						path: ['geo', row.geoid, key],
	    						value: key === 'type' ? getType(row) : row[key]
	    					})
	    				})

	    			})
		    		resolve(response);
		    	})
		    })
	    }
	}, // END GeoByGeoid

  // { route: "geo[{keys:geoids}].geom",
  //   get: function(pathSet) {
  //     const [, geoids] = pathSet;
  //     return GeoService.getGeoms(geoids)
  //       .then(rows => {
  //         return geoids.map(geoid => ({
  //           path: ["geo", geoid, "geom"],
  //           value: rows.reduce((a, c) => c.geoid === geoid ? c.geom : a, null)
  //         }))
  //       })
  //   }
  // },

	{
		route : `geo[{keys:geoids}]['state_abbr']`,
		get : function(pathSet){
			let response = [];
			var pathKeys = [pathSet[2]]
			return new Promise((resolve,reject) =>{
				let geoids = pathSet.geoids.map(d => d.toString())
				GeoService.fipsNameByFips(geoids).then(geodata => {
					// console.log('sheldusData', sheldusData.length)
					geodata.forEach(row => {
						pathKeys.forEach(key => {
							response.push({
								path: ['geo', row.geoid, key],
								value: key === 'type'
									? typeByGeoidLength[row.geoid.length]
									: row[key]
							})
						})

					})
					resolve(response);
				})
			})
		}
	},
	{ // ZipCodesByCounties
		route : `geo[{keys:geoids}].byZip['zip_codes']`,
		get : function(pathSet){
			let geoids = pathSet.geoids.map(d => d.toString())
			return  GeoService.ZipCodesByGeoidMem.then(fn => fn(geoids)).then(geodata => {
				let response = []
				geoids.forEach(geoid =>{
					response.push({
						path : ['geo',geoid,'byZip','zip_codes'],
						value : $atom(geodata.reduce((a,c) =>{
							if(c.geoid === geoid){
								a.push(c.zip_codes)
							}
							return a
						},[]))
					})
				})
				return response
			})

		}
	}, // END byZipCodes

  { route: "geo[{keys:geoids}].zcta",
    get: function(pathSet) {
      const geoids = pathSet.geoids.map(id => `${ id }`);
      return GeoService.ChildrenByGeoid(geoids, 'zcta')
        .then(geodata => {
          return geoids.map(geoid => ({
            path: ["geo", geoid, "zcta"],
            value: $atom(get(geodata, geoid, []))
          }))
        })
    }
  },
  { route: "geo[{keys:geoids}].unsd",
    get: function(pathSet) {
      const geoids = pathSet.geoids.map(id => `${ id }`);
      return GeoService.ChildrenByGeoid(geoids, 'unsd')
        .then(geodata => {
          return geoids.map(geoid => ({
            path: ["geo", geoid, "unsd"],
            value: $atom(get(geodata, geoid, []))
          }))
        })
    }
  },

	{ // CountiesByGeoid
		route: `geo[{keys:geoids}].counties`,
    get: function (pathSet) {
    	let response = [];
    	return new Promise((resolve, reject) => {
    		// force types
    		let geoids = pathSet.geoids.map(d => d.toString()) // for keys to string
    		GeoService.ChildrenByGeoid(geoids, 'county').then(geodata => {
  				geoids.forEach(geoid => {
  					response.push({
  						path: ['geo',geoid, 'counties'],
  						value: $atom(geodata[geoid])
  					})
    			})
	    		resolve(response);
	    	})
	    })
    }
	}, // END CountiesByGeoid

	{ // TractsByGeoid
		route: `geo[{keys:geoids}].tracts`,
	    get: function (pathSet) {
			// console.log('in func')
	    	let response = [];
	    	var pathKeys = pathSet[2]; // why? look into this
	    	return new Promise((resolve, reject) => {
	    		let geoids = pathSet.geoids.map(d => d.toString()) // for keys to string
	    		GeoService.ChildrenByGeoid(geoids, 'tract').then(geodata => {
    				geoids.forEach(geoid => {
    					response.push({
    						path: ['geo', geoid, 'tracts'],
    						value: $atom(geodata[geoid])
    					})
	    			})
		    		resolve(response);
		    	})
		    })
	    }
	}, // END TractsByGeoid

	{ // CousubsByGeoid
		route: `geo[{keys:geoids}].cousubs`,
	    get: function (pathSet) {
	    	let response = [];
	    	var pathKeys = pathSet[2]; // why? look into this
	    	return new Promise((resolve, reject) => {
	    		let geoids = pathSet.geoids.map(d => d.toString()) // for keys to string
	    		GeoService.ChildrenByGeoid(geoids, 'cousub').then(geodata => {
    				geoids.forEach(geoid => {
    					response.push({
    						path: ['geo', geoid, 'cousubs'],
    						value: $atom(geodata[geoid])
    					})
	    			})
		    		resolve(response);

		    	})
		    })
	    }
	}, // END CousubsByGeoid


    { // BlockGroupsByGeoid
        route: `geo[{keys:geoids}].blockgroup`,
        get: function (pathSet) {
            let response = [];
            var pathKeys = pathSet[2]; // why? look into this
            return new Promise((resolve, reject) => {
                let geoids = pathSet.geoids.map(d => d.toString()) // for keys to string
            GeoService.ChildrenByGeoid(geoids, 'blockgroup').then(geodata => {
                geoids.forEach(geoid => {
                response.push({
                path: ['geo', geoid, 'blockgroup'],
                value: $atom(geodata[geoid])
            })
        })
            resolve(response);

        })
        })
        }
    },
	{ // PLacesOrCousubsByCounty
		route: `geo[{keys:geoids}].municipalities`,
		get: function (pathSet) {
			let response = [];
			// force types
			let geoids = pathSet.geoids.map(d => d.toString()); // for keys to string
			return GeoService.placesOrCousubsByGeoid(geoids)
					.then(geodata => {
						geoids.forEach(geoid => {
							response.push({
								path: ['geo', geoid, 'municipalities'],
								value: $atom(geodata.filter(g => g.county_geo.toString() === geoid.toString()).map(g => g.geoid))
							})
						});
						return response
					})
		}
	}, // END CountiesByGeoid

  { route: 'geo[{keys:geoids}][{integers:years}][{keys:geolevels}]',
    get: function(pathSet) {
    	console.log('geo[{keys:geoids}][{integers:years}][{keys:geolevels}]')
      const [, geoids, years, geolevels] = pathSet;
      return GeoService.getIntersections(geoids, years, geolevels)
        .then(res => {
          const response = [];
          geoids.forEach(geoid => {
            const forGeoid = res.filter(r => r.geoid == geoid);
            years.forEach(year => {
              const forYear = forGeoid.filter(r => r.year == +`${ year.toString().slice(0, 3) }0`);
              geolevels.forEach(geolevel => {
                const forGeoLevel = forYear.filter(r => r.geolevel == get(GL_MAP, geolevel, geolevel));
                response.push({
                  path: ["geo", geoid, year, geolevel],
                  value: $atom(forGeoLevel.map(r => r.intersection))
                })
              })
            })
          })
          return response;
        })
    }
  },

	{ // CensusAcsByGeoidByYear
		route: `geo[{keys:geoids}][{keys:years}]['population', 'poverty', 'non_english_speaking', 'under_5', 'over_64', 'vulnerable', 'population_change', 'poverty_change', 'non_english_speaking_change', 'under_5_change', 'over_64_change', 'vulnerable_change']`,
		get: function(pathSet) {
			const geoids = pathSet.geoids.map(d => d.toString()),
				years = pathSet.years.map(d => +d);
			return GeoService.CensusAcsByGeoidByYear(geoids, years)
				.then(results => {
					const valueNames = pathSet[3];

					let returnData = [];

					pathSet.geoids.forEach(geoid => {
						pathSet.years.forEach(year => {
							valueNames.forEach(vn => {
								const path = ['geo', geoid, year, vn],
									result = results.reduce((a, c) => (c.geoid == geoid) && (c.year == year) ? c : a, null);
								returnData.push({
									value: result ? +result[vn] : 0,
									path
								})
							})
						})
					})
					// console.log(returnData)
					return returnData;
				})
		}

	} // END CensusAcsByGeoidByYear
	,
  {
    route: "geo.blockgroup.centroid",
    call: function(callPath, args) {
      return GeoService.getBlockgroupCentroid(args)
        .then(res => [
          {
            path: ["geo", "blockgroup", "centroid"],
            value: $atom(res)
          }
        ])
    }
  },
	{ // CensusAcsByGeoidBykeys census config
		route: `acs.config`,

		get: function(pathSet) {
				return[
					{
						path: ['acs','config'],
						value: $atom(CENSUS_CONFIG)
					}
				]

		}
	},
	{
		route : `geo.[{keys:geoids}].boundingBox`,
		get : function(pathSet){
			const geoids = pathSet.geoids.map(d => d.toString())
			return new Promise((resolve, reject) => {
				GeoService.getBoundingBoxByGeoid(geoids)
					.then(rows =>{
						const response = []
						geoids.forEach(geoid =>{
							const value = rows.reduce((a,c) => c.geoid === geoid ? c : a, null)
							if(value){
								response.push({
									path : ['geo',geoid,'boundingBox'],
									value : $atom(value.bounding_box)
								})
							}else{
								response.push({
									path : ['geo',geoid,'boundingBox'],
									value : $atom(null)
								})
							}
						})
						resolve(response)
					})
			})

		}
	},
	{
		route : `geo.[{keys:geoids}].geom`,
		get : function(pathSet){
			const geoids = pathSet.geoids.map(d => d.toString())
			return new Promise((resolve, reject) => {
				GeoService.getBoundingBoxByGeoid(geoids)
					.then(rows =>{
						const response = []
						geoids.forEach(geoid =>{
							const value = rows.reduce((a,c) => c.geoid === geoid ? c : a, null)
							if(value){
								response.push({
									path : ['geo',geoid,'geom'],
									value : $atom(value.geom)
								})
							}else{
								response.push({
									path : ['geo',geoid,'geom'],
									value : $atom(null)
								})
							}
						})
						resolve(response)
					})
			})

		}
	}

]//}
