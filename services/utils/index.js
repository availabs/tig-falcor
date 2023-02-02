module.exports = {
	getGeoidLengths: geoids =>
  		[...new Set(geoids.map(geoid => geoid.length)).values()]
}