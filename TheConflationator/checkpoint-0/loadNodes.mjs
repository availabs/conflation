import { pipeline, Transform } from "node:stream";
import split2 from "split2"
import pgCopyStreams from "pg-copy-streams";

import { format as d3format } from "d3-format"

const d3intFormat = d3format(",d");

function getNodeTransform(TheConflationator, node_insert_stmt) {
  const incAmt = 500000;
  let logInfoAt = incAmt;
  let numNodes = 0;
  return new Transform({
    transform(chunk, encoding, next) {
      const [id, lon, lat] = chunk.toString().split("|").map(Number);
      node_insert_stmt.run(id, lon, lat);
      if (++numNodes >= logInfoAt) {
        TheConflationator.logInfo("LOADED", d3intFormat(numNodes), "NODES");
        logInfoAt += incAmt;
      }
      next();
    }
  });
}
async function loadNodes(TheConflationator, client, OSM_nodes_table, node_insert_stmt) {
  const copyToStream = client.query(
    pgCopyStreams.to(`
      COPY ${ OSM_nodes_table }(osm_id, lon, lat)
      TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
    `)
  );
  TheConflationator.logInfo("LOADING NODES");
  await new Promise((resolve, reject) => {
    pipeline(
      copyToStream,
      split2(),
      getNodeTransform(TheConflationator, node_insert_stmt),
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
export default loadNodes;