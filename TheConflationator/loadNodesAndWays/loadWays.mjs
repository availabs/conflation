import { pipeline, Transform } from "node:stream";
import split2 from "split2"
import pgCopyStreams from "pg-copy-streams";

import * as turf from "@turf/turf";

import { format as d3format } from "d3-format"
import { dsvFormat as d3dsvFormat } from "d3-dsv"

const d3intFormat = d3format(",d");
const d3dsvFormatter = d3dsvFormat("|");

function getWayTransform(TheConflationator, edge_insert_stmt) {
  const incAmt = 50000;
  let logInfoAt = incAmt;
  let numWays = 0;
  const selectNodesSql = `
    SELECT *
      FROM nodes
      WHERE node_id IN (__NODE_IDs__)
  `;
  return new Transform({
    transform(chunk, encoding, next) {
      const [parsedRow] = d3dsvFormatter.parseRows(chunk.toString());
      const [id, refs, tags] = parsedRow.map(JSON.parse);

      const nodes = TheConflationator.db.all(selectNodesSql.replace("__NODE_IDs__", refs));
      const nodesMap = nodes.reduce((a, c) => { a[c.node_id] = [c.lon, c.lat]; return a; }, {});

      let needsForward = tags.oneway !== "-1";
      let needsReverse = (tags.oneway === "-1") || ((tags.oneway !== "yes") && (tags.highway !== "motorway"));

      if (needsForward) {

        for (let i = 1; i < refs.length; ++i) {

          const from_node = refs[i - 1];
          const from = nodesMap[from_node];

          const to_node = refs[i];
          const to = nodesMap[to_node];

          const bearing = turf.bearing(from, to);

          const length = turf.distance(from, to, { units: "miles" });

          edge_insert_stmt.run(id, i - 1, refs[i - 1], refs[i], bearing, length, tags.highway, 0);
        }
      }

      if (needsReverse) {

        refs.reverse();

        for (let i = 1; i < refs.length; ++i) {

          const from_node = refs[i - 1];
          const from = nodesMap[from_node];

          const to_node = refs[i];
          const to = nodesMap[to_node];

          const bearing = turf.bearing(from, to);

          const length = turf.distance(from, to, { units: "miles" });

          const isReversed = tags.oneway === "-1" ? 0 : 1;
          
          edge_insert_stmt.run(id, i - 1, refs[i - 1], refs[i], bearing, length, tags.highway, isReversed);
        }
      }

      if (++numWays >= logInfoAt) {
        TheConflationator.logInfo("LOADED", d3intFormat(numWays), "WAYS");
        logInfoAt += incAmt;
      }
      next();
    }
  });
}

async function loadWays(TheConflationator, client, OSM_ways_table, edge_insert_stmt) {
  const copyToStream = client.query(
    pgCopyStreams.to(`
      COPY (
        SELECT osm_id, ARRAY_TO_JSON(refs), tags
        FROM ${ OSM_ways_table }
      )
      TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
    `)
  );
  TheConflationator.logInfo("LOADING WAYS");
  await new Promise((resolve, reject) => {
    pipeline(
      copyToStream,
      split2(),
      getWayTransform(TheConflationator, edge_insert_stmt),
      error => {
        if (error) {
          reject(error);
        }
        else {
          resolve();
        }
      }
    )
  })
}
export default loadWays;