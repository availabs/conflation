import pgStuff from "pg";

import { format as d3format } from "d3-format"

const d3intFormat = d3format(",d");

import loadTMCs from "./loadTMCs.mjs"
import loadNodes from "./loadNodes.mjs"
import loadWays from "./loadWays.mjs"

async function runCheckpointZero(TheConflationator) {
    TheConflationator.logInfo("RUNNING CHECKPOINT ZERO");

    const { npmrds2, NPMRDS_view_id, OSM_view_id } = TheConflationator.config;

    TheConflationator.logInfo("CONNECTING CLIENT");
    const client = new pgStuff.Client(npmrds2);
    await client.connect();

    const [
      node_insert_stmt,
      edge_insert_stmt,
      tmc_insert_stmt
    ] = initializeCheckpointZero(TheConflationator);

    const NPMRDS_table = await getDataTable(client, NPMRDS_view_id);
    const OSM_ways_table = await getDataTable(client, OSM_view_id);
    const OSM_nodes_table = `${ OSM_ways_table }_nodes`;

    await loadTMCs(TheConflationator, client, NPMRDS_table, tmc_insert_stmt);
    await loadNodes(TheConflationator, client, OSM_nodes_table, node_insert_stmt);
    await loadWays(TheConflationator, client, OSM_ways_table, edge_insert_stmt);

    await client.end();

    // findIntersections(TheConflationator);

    reportStatsForCheckpointZero(TheConflationator);
}

export default runCheckpointZero;

async function getDataTable(client, viewId) {
	const sql = `
	  	SELECT data_table
		    FROM data_manager.views
		    WHERE view_id = $1
	`;
	const { rows: [{ data_table }]} = await client.query(sql, [viewId]);
	return data_table;
}

function initializeCheckpointZero(TheConflationator) {
  	TheConflationator.logInfo("INITIALIZING SQLITE DB FOR CHECKPOINT ZERO");

  	TheConflationator.db.run(`
    	CREATE TABLE IF NOT EXISTS nodes(
      		node_id INT PRIMARY KEY,
      		lon DOUBLE PRECISION NOT NULL,
      		lat DOUBLE PRECISION NOT NULL,
      		base_node_id INT
    	);
  	`);
  	const node_insert_stmt = TheConflationator.db.prepare("INSERT INTO nodes(node_id, lon, lat) VALUES(?, ?, ?);");

  	TheConflationator.db.run(`
    	CREATE TABLE IF NOT EXISTS edges(
			way_id INT NOT NULL,
			pos DOUBLE PRECISION NOT NULL,
			from_node INT NOT NULL,
			to_node INT NOT NULL,
			bearing DOUBLE PRECISION,
			length DOUBLE PRECISION,
			highway TEXT,
			reversed INT NOT NULL,
			PRIMARY KEY(from_node, to_node),
			FOREIGN KEY(from_node) REFERENCES nodes(node_id) ON DELETE CASCADE,
			FOREIGN KEY(to_node) REFERENCES nodes(node_id) ON DELETE CASCADE
		);
  	`);
  	const edge_insert_stmt = TheConflationator.db.prepare("INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, bearing, length, highway, reversed) VALUES(?, ?, ?, ?, ?, ?, ?, ?);");
  
  	const edgesWayIdIndexSql = `
    	CREATE INDEX edges_way_id_idx
      		ON edges(way_id);
  	`;
  	TheConflationator.db.run(edgesWayIdIndexSql);
  
  	const edgesFromNodeIndexSql = `
    	CREATE INDEX edges_from_node_idx
      		ON edges(from_node);
  	`;
  	TheConflationator.db.run(edgesFromNodeIndexSql);

  	const edgesToNodeIndexSql = `
    	CREATE INDEX edges_to_node_idx
      		ON edges(to_node);
  	`;
  	TheConflationator.db.run(edgesToNodeIndexSql);

	TheConflationator.db.run(`
    	CREATE TABLE IF NOT EXISTS tmcs(
			tmc TEXT,
			linestring_index INT,
			tmc_index INT,
			miles DOUBLE PRECISION,
			f_system INT,
			geojson TEXT,
			PRIMARY KEY(tmc, linestring_index, tmc_index)
    	);
  	`);
  	const tmc_insert_stmt = TheConflationator.db.prepare("INSERT INTO tmcs(tmc, linestring_index, tmc_index, miles, f_system, geojson) VALUES(?, ?, ?, ?, ?, ?);");

  	TheConflationator.db.run(`
    	CREATE TABLE IF NOT EXISTS intersections(
      		node_id INT PRIMARY KEY
    	);
  	`);

  	return [
	    node_insert_stmt,
	    edge_insert_stmt,
	    tmc_insert_stmt
  	]
}

async function reportStatsForCheckpointZero(TheConflationator) {

  	const tmcCountSql = `
	    SELECT COUNT(DISTINCT tmc) AS tmcs_count, COUNT(1) AS num_segments
	    FROM tmcs
  	`;
  	const { tmcs_count, num_segments } = TheConflationator.db.get(tmcCountSql);
  	TheConflationator.logInfo("LOADED", d3intFormat(num_segments), "SEGMENTS DERIVED FROM", d3intFormat(tmcs_count), "TMCs INTO SQLITE DB");
  	
  	const nodeCountSql = `
	    SELECT COUNT(1) AS nodes_count
	    FROM nodes
  	`;
  	const { nodes_count } = TheConflationator.db.get(nodeCountSql);
  	TheConflationator.logInfo("LOADED", d3intFormat(nodes_count), "NODES INTO SQLITE DB");

  	const edgeCountSql = `
	    SELECT COUNT(1) AS edges_count
	    FROM edges
  	`;
  	const { edges_count } = TheConflationator.db.get(edgeCountSql);
  	TheConflationator.logInfo("LOADED", d3intFormat(edges_count), "EDGES INTO SQLITE DB");

  	const intersectionCountSql = `
	    SELECT COUNT(1) AS intersections_count
	    FROM intersections
  	`;
  	const { intersections_count } = TheConflationator.db.get(intersectionCountSql);
  	TheConflationator.logInfo("LOADED", d3intFormat(intersections_count), "INTERSECTIONS INTO SQLITE DB");
}