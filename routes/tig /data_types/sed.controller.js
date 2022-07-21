const db_service = require("../../../services/tig_db");
const cachePath = require('../../../services/cache/cache-path');
const memoizeFs = require("memoize-fs");
const memoizer = memoizeFs({ cachePath });

const sedTazBySource = (source_ids) =>
    Promise.all(
      source_ids
        .reduce((a, c) => {
	        const sql = `
              SELECT name, area_id, value, st_asgeojson(geom) as geom, enclosing_name, enclosing_type, ${c} as source_id 
              FROM sed_taz.sed_taz_source_${c}
            `;
       	    a.push(db_service.promise(sql));
          return a;
        }, [])
    ).then(res => {
    	return [].concat(...res)
    })


module.exports = {
	sedTazBySource,
  sedTazBySourceMem: memoizer.fn(sedTazBySource),
}