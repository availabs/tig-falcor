const CENSUS_DATA_API_KEY = require("./censusDataApiKey");

const EARLIEST_DATA_YEAR = 2009;
const LATEST_DATA_YEAR = 2016;

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
// used to slice API response row and sum required variables
const CENSUS_API_SLICES = {};
// EXAMPLE SLICE: { "under_5": [3, 5] }

let count = 0;
CENSUS_API_VARIABLES_BY_GROUP.forEach(group => {
	CENSUS_API_VARIABLE_NAMES.push(group.name);
	CENSUS_API_VARIABLES.push(...group.variables);
	const length = group.variables.length;
	CENSUS_API_SLICES[group.name] = [count, count + length];
	count += length
})

const makeBaseCensusApiUrl = year => {
	if (!AVAILABLE_DATA_YEARS[year]) return null;
	return "https://api.census.gov/data/" +
		`${ year }/` +
		`${ (year > 2014) ? 'acs/' : '' }` +
		`acs5?` + 
		`key=${ CENSUS_DATA_API_KEY }` +
		`&get=${ CENSUS_API_VARIABLES }`
}

class Geoid {
	constructor(geoid) {
		geoid = geoid.toString();

		this.length = geoid.length;

		this.state = geoid.slice(0, 2);

		this.county = null;
		this.cousub = null;
		this.tract = null;

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
		}
	}
	makeUrlAndKey(year) {
		let url = makeBaseCensusApiUrl(year), key;
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
			}
		}
		return { url, key };
	}
}

const sumSlices = (row, i, j) =>
	row.slice(i, j)
		.reduce((a, c) => a + +c, 0)
const makeGeoid = row =>
	row.slice(CENSUS_API_VARIABLES.length)
		.sort((a, b) => a.length - b.length)
		.join("")

module.exports = {
	fillCensusApiUrlArray: (geoids, years) => {
		let urlMap = {};
		geoids.forEach(geoid => {
			years.forEach(year => {
				const geoidObj = new Geoid(geoid),
					{ url, key } = geoidObj.makeUrlAndKey(year);
				urlMap[key] = [year, url];
			})
		})
		return Object.values(urlMap).filter(([year, url]) => Boolean(url));
	},

	CENSUS_API_VARIABLE_NAMES,

	processCensusApiRow: (row, year) => {
		const data = {
		    geoid: makeGeoid(row),
		    year
		};
		for (const key in CENSUS_API_SLICES) {
			data[key] = sumSlices(row, ...CENSUS_API_SLICES[key]);
		}
	    return data;
	}
}