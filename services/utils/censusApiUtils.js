const CENSUS_KEY_CONFIG = require('./censusConfig')
const CENSUS_DATA_API_KEY = null;//require("./censusDataApiKey");
const EARLIEST_DATA_YEAR = 2009;
const LATEST_DATA_YEAR = 2017;
let subvariables =[]
let AVAILABLE_DATA_YEARS = {};
for (let i = EARLIEST_DATA_YEAR; i <= LATEST_DATA_YEAR; ++i) {
	AVAILABLE_DATA_YEARS[i] = true;
}

const CENSUS_API_VARIABLES_BY_GROUP = [
	{ name: "population",
		variables: [
			'B01003_001E'	// total population
		]
	},

	/*
	{ name: "poverty",
		variables: [
			'B16009_001E'	// total population at poverty level
		]
	},

	{ name: "non_english_speaking",
		variables: [
			'B06007_005E'	// total population that speaks english less than "very well"
		]
	},

	{ name: "under_5",
		variables: [
			'B01001_003E',	// males under 5
			'B01001_027E'	// females under 5
		]
	},
    */
	{ name: "65_and_over",
		variables: [
			'B01001_020E',	// males 65 & 66
			'B01001_021E',	// males 67 - 69
			'B01001_022E',	// males 70 - 74
			'B01001_023E',	// males 75 - 79
			'B01001_024E',	// males 80 - 84
			'B01001_025E',	// males 85+
			'B01001_044E',	// females 65 & 66
			'B01001_045E',	// females 67 - 69
			'B01001_046E',	// females 70 - 74
			'B01001_047E',	// females 75 - 79
			'B01001_048E',	// females 80 - 84
			'B01001_049E'	// females 85+
		]
	}

]

const CENSUS_API_VARIABLE_NAMES = [];
const CENSUS_API_VARIABLES = [];
const CENSUS_API_COUNTIES =['36001','36083','36093','36091','36039','360021','36115','36113']
// used to slice API response row and sum required variables
const CENSUS_API_SLICES = {};
// EXAMPLE SLICE: { "under_5": [3, 5] }

let count = 0;


Object.values(CENSUS_KEY_CONFIG).forEach(censusKey => {
    CENSUS_API_VARIABLE_NAMES.push(censusKey.name);
	CENSUS_API_VARIABLES.push(...censusKey.variables);
	const length = censusKey.variables.length;
	CENSUS_API_SLICES[censusKey.variables.map(d => d.value)] = [count,count + length];// changed here
	count += length
})

/*
 CENSUS_API_VARIABLES_BY_GROUP.forEach(group => {
 	CENSUS_API_VARIABLE_NAMES.push(group.name);
 	CENSUS_API_VARIABLES.push(...group.variables);
	const length = group.variables.length;
	CENSUS_API_SLICES[group.variables] = [count, count + length];//changed here
	count += length


 })
console.log('CENSUS_API_VARIABLES',CENSUS_API_VARIABLES)
console.log('CENSUS_API_SLICES',CENSUS_API_SLICES)
 */


const makeBaseCensusApiUrl = (year, censusKeys) =>
{

    if (!AVAILABLE_DATA_YEARS[year]) return null;
    let CENSUS_VARIABLES = censusKeys.map(key => {
    return [].concat(...CENSUS_KEY_CONFIG[key].variables.map(d => d.value)) //Should be B001003_001E

})

    let url = "https://api.census.gov/data/" +
        `${ year }` +
        `${(year >= 2010) ? '/acs/' : ''}`+
        `acs5?` +
        `&get=${CENSUS_VARIABLES}`+
        `&key=${CENSUS_DATA_API_KEY}`

    return url
}



class Geoid {
	constructor(geoid) {
		geoid = geoid.toString();

		this.length = geoid.length;

		this.state = geoid.slice(0, 2);

		this.county = null;
		this.cousub = null;
		this.tract = null;
		this.blockgroup = null;

		switch (geoid.length) {
			case 5:
				this.county = geoid.slice(2);
				break;
			case 10:
				this.county = geoid.slice(2, 5);
				this.cousub = geoid.slice(5);
				break;
			case 11:
				this.county = geoid.slice(2, 5);
				this.tract = geoid.slice(5);
				break;
			case 12:
				this.county = geoid.slice(2,5);
				this.blockgroup = geoid.slice(5);

		}
	}
	makeUrlAndKey(year, censusKeys) {
		let url = makeBaseCensusApiUrl(year,censusKeys), key

		if (url !== null) {
			switch (this.length) {
				case 2:
					url += `&for=state:${ this.state }`;
					key = `${ year }-state-${ this.state }`;
					break;
				case 5:
					url += `&for=county:*`;
					url += `&in=state:${ this.state }`;
					key = `${ year }-counties-${ this.state }`;
					break;
				case 10:
					url += `&for=county+subdivision:*`
					url += `&in=state:${ this.state }+county:${ this.county }`
					key = `${ year }-cousubs-${ this.state }-${ this.county }`;
					break;
				case 11:
					url += `&for=tract:*`
					url += `&in=state:${ this.state }+county:${ this.county }`
					key = `${ year }-tracts-${ this.state }-${ this.county }`;
					break;
				case 12:
					url += `&for=block+group:*`
					url += `&in=state:${ this.state }+county:${ this.county }`;
					key =  `${ year }-blockgroup-${ this.state }-${ this.county }`;
			}
		}
		return { url, key };
	}
}

const sumSlices = (row, i, j) =>
	row.slice(i, j)
		.reduce((a, c) => a + +c, 0)
const makeGeoid = row =>
	// console.log('row',row)
	//row.slice(CENSUS_API_COUNTIES.length)
		//.sort((a, b) => a.length - b.length)
		//.join("")
module.exports = {
	fillCensusApiUrlArray: (geoids, years, censusKeys) => {
		let urlMap = {};
		geoids.forEach(geoid => {
			years.forEach(year => {
				const geoidObj = new Geoid(geoid),
					{ url, key } = geoidObj.makeUrlAndKey(year, censusKeys);
				urlMap[url] = [year, url];
			})
		})
		return Object.values(urlMap).filter(([year, url]) => Boolean(url));
	},

	CENSUS_API_VARIABLE_NAMES,
	processCensusApiRow: (row,county, year,censusKeys) => {
		const data = {
            //geoid: '36001',
		    geoid: county,
		    year
		};
        for (const key in CENSUS_API_SLICES)
		{
			var multi_key = key.split(',');
			multi_key.forEach(function(i,index){
				let censusCat = i.split('_')[0];
				if (Object.keys(censusKeys).length != 1){ // if you call only one census key
                    censusKeys.forEach(function(item){
                        let cenKey = item;
                        if(censusCat === cenKey){
                            if (!data[censusCat]) {
                                data[censusCat] = {}

                            }

                            let dataLocation = CENSUS_API_SLICES[key][0] + index
                            data[censusCat][i] = row[dataLocation]
                        }

                    })
				}
				else{
                    censusKeys.forEach(function(item){ // if you call multiple census keys
                        let cenKey = item;
                        if(censusCat === cenKey){
                            if (!data[censusCat]) {
                                data[censusCat] = {}

                            }
                            let dataLocation = CENSUS_API_SLICES[key][0] + (index - CENSUS_API_SLICES[key][0])
                            data[censusCat][i] = row[dataLocation]
                        }

                    })
				}



			})


			}

    		return data;
    },    
	EARLIEST_DATA_YEAR,
	LATEST_DATA_YEAR


                //console.log(...CENSUS_API_SLICES[key]
}
