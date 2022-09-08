const db_service = require("../../../../services/tig_db");
const cachePath = require('../../../../services/cache/cache-path');
const memoizeFs = require("memoize-fs");
const memoizer = memoizeFs({ cachePath });

const tigHubBoundTravelDatabyViewID = (viewIDs) =>{
    const sql = `
        SELECT count,
               hour,
               cf.id,
               l.latitude  lat,
               l.longitude lon,
               location_id loc_id,
               l.name      loc_name,
               l_in.name in_station_name, 
               l_out.name out_station_name,
               m.name      mode_name,
               tr.name     route_name,
               s.name      sector_name,
               cv.name     var_name,
               ta.name     transit_agency,
               year,
               direction,
               view_id
        FROM public.count_facts cf
                 left outer JOIN count_variables cv
                      ON count_variable_id = cv.id
                 left outer JOIN sectors s
                      ON sector_id = s.id
                 left outer JOIN transit_routes tr
                      ON transit_route_id = tr.id
                 left outer JOIN transit_modes m
                      ON transit_mode_id = m.id
                 left outer JOIN locations l
                      ON location_id = l.id
                 left outer JOIN locations l_in
                      ON in_station_id = l_in.id
                 left outer JOIN locations l_out
                      ON out_station_id = l_out.id
                 left outer JOIN transit_agencies ta
                      ON cf.transit_agency_id = ta.id
        where view_id IN ('${viewIDs.join(`','`)}')
    `;

    return db_service.promise(sql);
}


module.exports = {
	tigHubBoundTravelDatabyViewID,
  tigHubBoundTravelDatabyViewIDMem: memoizer.fn(tigHubBoundTravelDatabyViewID),
}