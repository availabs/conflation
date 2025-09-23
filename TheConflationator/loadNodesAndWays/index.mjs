import pgStuff from "pg";

import { D3_INT_FORMAT } from "../constants.mjs"

import initializeCheckpoint from "./initializeCheckpoint.mjs"
import loadNodes from "./loadNodes.mjs"
import loadWays from "./loadWays.mjs"

async function runCheckpoint(TheConflationator) {

  const { OSM_view_id } = TheConflationator.config;

  const client = await TheConflationator.getClient();

  const [
    node_insert_stmt,
    edge_insert_stmt
  ] = initializeCheckpoint(TheConflationator);

  const OSM_ways_table = await TheConflationator.getDataTable(client, OSM_view_id);
  const OSM_nodes_table = `${ OSM_ways_table }_nodes`;

  await loadNodes(TheConflationator, client, OSM_nodes_table, node_insert_stmt);
  await loadWays(TheConflationator, client, OSM_ways_table, edge_insert_stmt);

  await client.end();

  findIntersections(TheConflationator);

  reportStatsForCheckpointZero(TheConflationator);
}
export default runCheckpoint;

function findIntersections(TheConflationator) {
  const sql = `
    WITH
      edges_cte AS (
        SELECT
            from_node AS node_id,
            way_id,
            reversed
          FROM edges

        UNION

        SELECT
            to_node AS node_id,
            way_id,
            reversed
          FROM edges
      ),
      way_counts AS (
        SELECT node_id, COUNT(DISTINCT way_id) AS num_ways
          FROM edges_cte
          GROUP BY 1
      ),
      reversed_nodes AS (
        SELECT DISTINCT node_id
          FROM edges_cte
          WHERE reversed = 1
      )
    INSERT INTO intersections(node_id)
      SELECT DISTINCT node_id
        FROM way_counts
          JOIN reversed_nodes
            USING(node_id)
        WHERE num_ways > 2;
  `;
  TheConflationator.logInfo("FINDING INTERSECTIONS");
  TheConflationator.db.run(sql);
}

function reportStatsForCheckpointZero(TheConflationator) {
	const nodeCountSql = `
    SELECT COUNT(1) AS nodes_count
    FROM nodes
	`;
	const { nodes_count } = TheConflationator.db.get(nodeCountSql);
	TheConflationator.logInfo("LOADED", D3_INT_FORMAT(nodes_count), "NODES INTO SQLITE DB");

	const edgeCountSql = `
    SELECT COUNT(1) AS edges_count
    FROM edges
	`;
	const { edges_count } = TheConflationator.db.get(edgeCountSql);
	TheConflationator.logInfo("LOADED", D3_INT_FORMAT(edges_count), "EDGES INTO SQLITE DB");

	const intersectionCountSql = `
    SELECT COUNT(1) AS intersections_count
    FROM intersections
	`;
	const { intersections_count } = TheConflationator.db.get(intersectionCountSql);
	TheConflationator.logInfo("LOADED", D3_INT_FORMAT(intersections_count), "INTERSECTIONS INTO SQLITE DB");
}