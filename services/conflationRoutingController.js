const { Transform } = require('readable-stream');
const { pipeline } = require("stream"),
  split = require("split2");

const flatten = require("lodash.flatten")

const concaveman = require("concaveman")

const { writeFileSync } = require("fs")

const { to: copyTo } = require('pg-copy-streams');

const { npmrds_db: db_service, npmrdsClient } = require("../db_service");

const { rollup: d3rollup, mean: d3mean } = require("d3-array");

const turf = require("@turf/turf")

// const db_service = require('./npmrds_db');

const createGraph = require("ngraph.graph"),
  { aStar } = require("ngraph.path");

const CONFLATION_VERSION = "v0_6_0"

let NODES = [];
let WAYS = [];
let GRAPH = createGraph();
let AVG_SPEED_LIMIT_BY_NETWORK_LEVEL = new Map();
let TMC_META = new Map();

const streamNodes = async client => {

  const transformer = new Transform({
    transform(chunk, enc, callback) {
      const [id, geoJSON] = chunk.toString().split("|");
      const geom = JSON.parse(geoJSON);
      GRAPH.addNode(+id, { coords: geom.coordinates });
      callback(null);
    }
  });

  await new Promise((resolve, reject) => {
    pipeline(
      client.query(
        copyTo(`
          COPY (
            SELECT id, ST_AsGeoJSON(wkb_geometry) AS geom
            FROM conflation.conflation_map_2020_nodes_${ CONFLATION_VERSION }
            WHERE id IN (
              SELECT unnest(a.node_ids)
              FROM conflation.conflation_map_2020_ways_${ CONFLATION_VERSION } AS a
                JOIN conflation.conflation_map_2020_${ CONFLATION_VERSION } AS b
                  USING(id)
              WHERE n < 7
            )
          )
          TO STDOUT WITH (FORMAT TEXT, DELIMITER '|')`)
      ),
      split(),
      transformer,
      err => {
        if (err) {
          reject(err);
        } else {
          resolve(null);
        }
      }
    )
  });
}

const streamWays = async client => {

  const transformer = new Transform({
    transform(chunk, enc, callback) {
      const [id, nodesJSON, tmc, n] = chunk.toString().split("|");
      const nodes = JSON.parse(nodesJSON);
      for (let i = 1; i < nodes.length; ++i) {
        if (nodes[i - 1] !== nodes[i]) {
          GRAPH.addLink(+nodes[i - 1], +nodes[i], { wayId: id, tmc, n });
        }
      }
      callback(null);
    }
  });

  return new Promise((resolve, reject) => {
    pipeline(
      client.query(
        copyTo(`
          COPY (
            SELECT a.id, array_to_json(a.node_ids), b.tmc, b.n
            FROM conflation.conflation_map_2020_ways_${ CONFLATION_VERSION } AS a
              JOIN conflation.conflation_map_2020_${ CONFLATION_VERSION } AS b
                USING(id)
            WHERE n < 7
          )
          TO STDOUT WITH (FORMAT CSV, DELIMITER '|')`
        )
      ),
      split(),
      transformer,
      err => {
        if (err) {
          reject(err);
        }
        else {
          resolve(null);
        }
      }
    )
  })
  .catch(error => {
    console.log("There was an error streaming ways:", error);
  });

}

const getAvgSpeedLimitByNetworkLevel = async () => {
  const sql = `
    SELECT n, tags->>'maxspeed' AS maxspeed
    FROM conflation.conflation_map_2020_${ CONFLATION_VERSION } AS c
      JOIN osm.osm_ways_v200101 AS o
        ON c.osm = o.id
    WHERE o.tags->>'maxspeed' IS NOT NULL;
  `
  const data = await db_service.promise(sql);

  const reducer = group => {
    return d3mean(group, d => parseInt(d.maxspeed)) / 3600.0;
  }

  AVG_SPEED_LIMIT_BY_NETWORK_LEVEL = d3rollup(data, reducer, d => +d.n);
}

const getTmcMeta = async () => {
  const sql = `
    SELECT tmc, miles
    FROM tmc_metadata_2020
  `
  TMC_META = await db_service.promise(sql)
    .then(rows => rows.reduce((a, c) => {
      a.set(c.tmc, c.miles);
      return a;
    }, new Map()));
}

const getNpmrds = async (point, miles, startDate, endDate, startTime, endTime) => {
  const sql = `
    SELECT tmc, AVG(travel_time_all_vehicles) AS tt
    FROM npmrds
    WHERE date >= $1 AND date <= $2
    AND epoch >= $3 AND epoch < $4
    AND tmc = ANY(
      SELECT DISTINCT tmc
      FROM conflation.conflation_map_2020_${ CONFLATION_VERSION }
      WHERE n < 7
      AND ST_DWithin(
        ST_Transform(wkb_geometry, 2877),
        ST_Transform('SRID=4326;POINT(${ +point.lng } ${ +point.lat })'::geometry, 2877),
        ${ +miles } * 1609.34
      )
    )
    GROUP BY 1
  `
  const rows = await db_service.promise(sql, [startDate, endDate, startTime, endTime]);

  return rows.reduce((a, c) => {
    a.set(c.tmc, c.tt);
    return a;
  }, new Map());
}

const getNodesFromCoords = async coords => {
  const promises = coords.map((lngLat, i) => {
    const sql = `
      SELECT id, ${ i } AS index
      FROM conflation.conflation_map_2020_nodes_${ CONFLATION_VERSION }
      WHERE id IN (
        SELECT UNNEST(node_ids)
        FROM conflation.conflation_map_2020_${ CONFLATION_VERSION } AS m
          JOIN conflation.conflation_map_2020_ways_${ CONFLATION_VERSION } AS w
            USING(id)
        WHERE m.n < 7
      )
      ORDER BY wkb_geometry <-> 'SRID=4326;POINT(${ +lngLat.lng } ${ +lngLat.lat })'::geometry
      LIMIT 1
    `
    return db_service.promise(sql)
      .then(rows => rows.pop())
  })
  const rows = await Promise.all(promises);

  return rows.sort((a, b) => +a.index - +b.index).map(({ id }) => +id);
}

const getTravelTime = (tmc, NPMRDS, miles, n) => {
  if (NPMRDS.has(tmc) && TMC_META.has(tmc)) {
    const tmcMiles = TMC_META.get(tmc);
    return NPMRDS.get(tmc) * (miles / tmcMiles);
  }
  return miles / AVG_SPEED_LIMIT_BY_NETWORK_LEVEL.get(+n);
}

const walkGraph = (startNode, durations, NPMRDS) => {
  const requests = [{
    nodeId: startNode,
    travelTime: 0
  }];

  const linksForDurations = durations.map(() => new Map());

  const visitedNodes = new Map();

  visitedNodes.set(startNode, 0);

  let index = 0;

  while (index < requests.length) {
    const { nodeId, travelTime } = requests[index++];

    const node = GRAPH.getNode(+nodeId);

    if (node) {
      node.links
        .filter(({ fromId }) => +fromId === +nodeId)
        .forEach(({ toId, data }) => {
          const toNode = GRAPH.getNode(+toId);

          const { tmc, n } = data;

          const miles = turf.distance(node.data.coords, toNode.data.coords, { units: "miles" });

          const tt = travelTime + getTravelTime(tmc, NPMRDS, miles, n);

          for (const i in durations) {
            const duration = durations[i];
            if ((tt < duration) &&
                (!visitedNodes.has(+toId) || (visitedNodes.get(+toId) > tt))
              ) {

              visitedNodes.set(+toId, tt);

              linksForDurations[i].set(toId, toNode.data.coords);
              requests.push({
                nodeId: +toId,
                travelTime: tt
              });
              break;
            }
          }
        })
    }
  }

  return linksForDurations;
}

module.exports = {

  loadConflationRoutingData: async () => {
console.log(`LOADING GRAPH DATA FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED LOADING GRAPH DATA FOR PROCESS: ${ process.pid }`);

    const client = await npmrdsClient();

    try {

      GRAPH = createGraph();

console.log(`STREAMING NODES FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED STREAMING NODES FOR PROCESS: ${ process.pid }`);
      await streamNodes(client);
console.timeEnd(`FINISHED STREAMING NODES FOR PROCESS: ${ process.pid }`);

console.log(`STREAMING WAYS FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED STREAMING WAYS FOR PROCESS: ${ process.pid }`);
      await streamWays(client);
console.timeEnd(`FINISHED STREAMING WAYS FOR PROCESS: ${ process.pid }`);

console.log(`LOADING AVG SPEED LIMITS FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED LOADING AVG SPEED LIMITS FOR PROCESS: ${ process.pid }`);
      await getAvgSpeedLimitByNetworkLevel();
console.timeEnd(`FINISHED LOADING AVG SPEED LIMITS FOR PROCESS: ${ process.pid }`);

console.log(`LOADING TMC META FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED LOADING TMC META FOR PROCESS: ${ process.pid }`);
      await getTmcMeta();
console.timeEnd(`FINISHED LOADING TMC META FOR PROCESS: ${ process.pid }`);

    }
    catch (e) {
      console.log("ERROR:", e);
    }
    finally {
      client.end();
    }

console.timeEnd(`FINISHED LOADING GRAPH DATA FOR PROCESS: ${ process.pid }`);
  },

  generateIsochrone: async request => {

    const {
      startPoint,
      startTime = 144,
      startDate = "01-01-2020",
      endDate = "12-31-2020",
      weekdays = [1, 2, 3, 4, 5],
      durations = [5, 15, 30]
    } = request;

    const maxDuration = durations[durations.length - 1],
      epochs = Math.ceil(maxDuration / 5),
      miles = maxDuration * (75 / 60);

    const [startNode] = await getNodesFromCoords([startPoint]);

    if (!startNode) return null;

    const NPMRDS = await getNpmrds(startPoint, miles,
                                    startDate, endDate,
                                    startTime, startTime + epochs);

    const data = await walkGraph(startNode, durations.map(d => d * 60), NPMRDS);

    const [features] = data.reduce((a, c, i) => {
      const polygon = concaveman([...c.values(), ...a[1]], 1, 0)
      a[0].push({
        type: "Feature",
        properties: { time: durations[i] },
        geometry: {
          type: "Polygon",
          coordinates: [polygon]
        }
      });
      a[1].push(...polygon);
      return a;
    }, [[], []]);

    const collection = {
      type: "FeatureCollection",
      features
    }
    // writeFileSync("test_isochrone.geojson", JSON.stringify(collection, null, 3));

    return collection;
  },

  getRoute: async coords => {
console.log("COORDS:", coords);
    const pathFinder = aStar(GRAPH, {
      oriented: true,
/*
      distance: (form, to, link) {
        const { tmc } = link.data;


      }
*/
    });

    const nodes = await getNodesFromCoords(coords);
console.log("NODES:", nodes);

    const path = [];
    for (let i = 0; i < (nodes.length - 1); ++i) {
      path.push(...pathFinder.find(nodes[i], nodes[i + 1]));
    }

    path.reverse();

    const route = [];

    for (let i = 0; i < path.length - 1; ++i) {
      const from = path[i].id,
        to = path[i + 1].id,
        links = path[i].links;
      const linkId = links.reduce((a, c) => {
        return c.fromId === from && c.toId === to ? c.data.id : a;
      }, null)
      if (linkId && !route.includes(linkId)) {
        route.push(linkId);
      };
    }
    return route;
  }
}
