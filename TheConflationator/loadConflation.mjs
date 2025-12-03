import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';
import { pipeline, Readable } from "node:stream";
import {
	copyFileSync,
	mkdirSync,
	rmSync
} from "node:fs"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import { format as d3format } from "d3-format"
import {
	group as d3group,
	groups as d3groups,
	rollup as d3rollup,
	rollups as d3rollups
} from "d3-array"

import * as turf from "@turf/turf";

import SQLite3DB from "./BetterSQLite3DB.mjs";

import setDamaTables from "../setDamaTables.mjs";

import {
	HIGHWAY_TO_F_SYSTEM_MAP
} from "./constants.mjs"

import config from "../config.js"
const { db_info } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const WORKING_DIRECTORY = join(__dirname, "working_directory");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

const SQLITE_FILE_PATH = join(__dirname, "checkpoints", "checkpoint-6.sqlite");

const d3intFormat = d3format(",d");

const logInfo = (...args) => {
	const string = args.reduce((a, c) => {
	  if (typeof c === "object") {
	    return `${ a } ${ JSON.stringify(c) }`;
	  }
	  return `${ a } ${ c }`
	}, `${ new Date().toLocaleString() }:`);
	console.log(string);
}

class WayBucket {
	constructor(way_index) {
		this.way_index = +way_index;
		this.edges = [];
	}
	pushEdge(newEdge) {
		if (+newEdge.way_index !== this.way_index) return false;

		for (const edge of this.edges) {
			if (newEdge.ris_id && !edge.ris_id) {
				edge.ris_id = newEdge.ris_id;
				// edge.ris_index = newEdge.ris_index;
				return true;
			}
			else if (newEdge.tmc && !edge.tmc) {
				edge.tmc = newEdge.tmc;
				// edge.tmc_index = newEdge.tmc_index;
				return true;
			}
		}

		this.edges.push({
			way_id: newEdge.way_id,
			way_index: newEdge.way_index,

			from_node: newEdge.from_node,
			to_node: newEdge.to_node,

			reversed: newEdge.reversed,
			highway: newEdge.highway,

			tmc: newEdge.tmc,
			// tmc_index: newEdge.tmc_index,

			ris_id: newEdge.ris_id,
			// ris_index: newEdge.ris_index
		});

		return true;
	}
	popEdge(edge) {
		if (!edge) return this.edges.shift();
		const edgeKey = `${ edge.tmc }-${ edge.ris_id }`;
		const index = this.edges.findIndex(edge => edgeKey === `${ edge.tmc }-${ edge.ris_id }`);
		if (index === -1) return null;
		return this.edges.splice(index, 1).pop();
	}
}

(async () => {
  rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
  mkdirSync(WORKING_DIRECTORY);

	logInfo("LOADING SQLITE DB FROM", SQLITE_FILE_PATH);
	copyFileSync(SQLITE_FILE_PATH, ACTIVE_DB_PATH);

	const db = new SQLite3DB(ACTIVE_DB_PATH);

	logInfo("CONNECTING CLIENT");
  const client = new pgStuff.Client(db_info);
  await client.connect();
	logInfo("CLIENT CONNECTED");

	// const createConflationTableSql = `
	// 	BEGIN;

	// 	DROP TABLE IF EXISTS osm_datasets.osm_conflation_1;

	// 	CREATE TABLE osm_datasets.osm_conflation_1(
	// 		ogc_fid BIGSERIAL PRIMARY KEY,
	// 		osm BIGINT,
	// 		ris TEXT,
	// 		tmc TEXT,
	// 		year INT,
	// 		dir INT,
	// 		n INT,
	// 		osm_fwd INT, --1 for non-reversed, - for reversed
	// 		miles DOUBLE PRECISION,
	// 		wkb_geometry GEOMETRY(LineString, 4032)
	// 	);

	// 	COMMIT;
	// `;

	// await client.query(createConflationTableSql);
	// logInfo("CREATED CONFLATION TABLE: osm_datasets.osm_conflation_1");

	// const queryConflationEdgesSql = `
	// 	WITH conflated_edges AS (
	// 		SELECT
	// 				tmc,

	// 				'no-ris' AS ris_id,

	// 				from_node,
	// 				to_node

	// 			FROM npmrds_conflation

	// 		UNION

	// 		SELECT
	// 				'no-tmc' AS tmc,

	// 				ris_id,

	// 				from_node,
	// 				to_node

	// 			FROM ris_conflation
	// 	)
	// 	SELECT
	// 			e.way_id,
	// 			e.pos AS way_index,

	// 			COALESCE(c.tmc, 'no-tmc') AS tmc,

	// 			COALESCE(c.ris_id, 'no-ris') AS ris_id,

	// 			e.from_node,
	// 			e.to_node,

	// 			e.highway,
	// 			e.reversed

	// 		FROM edges AS e
	// 			LEFT JOIN conflated_edges AS c
	// 				ON e.from_node = c.from_node
	// 				AND e.to_node = c.to_node
	// `;
	// logInfo("LOADING CONFLATION EDGES");
	// const conflationEdges = db.all(queryConflationEdgesSql);
	// logInfo("LOADED", d3intFormat(conflationEdges.length), "CONFLATION EDGES");

	// logInfo("STREAMING CONFLATION EDGES");
	// await generateMainConflationTable(conflationEdges, client, db);
	// logInfo("COMPLETED STREAMING CONFLATION EDGES");

	// const damaArgs = [
	// 	'OSM Conflation 1.0',
	// 	'gis_dataset',
	// 	'osm_datasets.osm_conflation_1',
	// 	[['OSM Conflation', 'Road Network']]
	// ];
	// await setDamaTables(client, ...damaArgs);

	const dropTablesSql = `
		BEGIN;

		DROP TABLE IF EXISTS osm_datasets.osm_conflation_1_edges;

		DROP TABLE IF EXISTS osm_datasets.osm_conflation_1_nodes;

		END;
	`
	await client.query(dropTablesSql);

	await generateNodesTable(client, db);
	await generateEdgesTable(client, db);

	db.close();
	await client.end();

  rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()

const generateEdgesTable = async (client, db) => {
	const createTableSql = `
		CREATE TABLE osm_datasets.osm_conflation_1_edges(
			ogc_fid BIGSERIAL PRIMARY KEY,
			osm BIGINT,
			tmc TEXT,
			ris TEXT,
			highway TEXT,
			reversed BOOLEAN,
			from_node BIGINT,
			to_node BIGINT,
			wkb_geometry GEOMETRY(LINESTRING, 4326),
			FOREIGN KEY(from_node) REFERENCES osm_datasets.osm_conflation_1_nodes(osm_id) ON DELETE CASCADE,
			FOREIGN KEY(to_node) REFERENCES osm_datasets.osm_conflation_1_nodes(osm_id) ON DELETE CASCADE
		);
	`;
	await client.query(createTableSql);

	const queryEdgesSql = `
		WITH conflated_edges AS (
			SELECT
					tmc,
					ris_id,
					n.from_node,
					n.to_node
				FROM npmrds_conflation AS n
					FULL OUTER JOIN ris_conflation AS r
						ON n.from_node = r.from_node
						AND n.to_node = r.to_node
		)
		SELECT
				e.way_id,
				tmc,
				ris_id,
				e.highway,
				e.reversed,
				e.from_node,
				e.to_node
		FROM edges AS e
			LEFT JOIN conflated_edges AS c
				ON e.from_node = c.from_node
				AND e.to_node = c.to_node
	`;
	const edges = db.all(queryEdgesSql);

	logInfo("LOADING NODES");
	const nodes = db.all("SELECT * FROM nodes;");
	logInfo("LOADED", d3intFormat(nodes.length), "NODES");
	const nodesMap = nodes.reduce((a, c) => {
		a.set(c.node_id, [+c.lon, +c.lat]);
		return a;
	}, new Map());

	const incAmt = 500000;
	let logInfoAt = incAmt;
	let numInserted = 0;

	async function* yieldEdges() {
		for (const edge of edges) {
			const {
				way_id,
				tmc,
				ris_id,
				highway,
				reversed,
				from_node,
				to_node
			} = edge;

			const fromNode = nodesMap.get(from_node);
			const toNode = nodesMap.get(to_node);

			const geojson = JSON.stringify({
				type: "LineString",
				coordinates: [fromNode, toNode]
			});
			const values = [
				way_id,
				tmc,
				ris_id,
				highway,
				reversed,
				from_node,
				to_node,
				geojson
			];

			yield `${ values.join("|") }\n`;

			if (++numInserted >= logInfoAt) {
				logInfo("INSERTED", d3intFormat(numInserted), "EDGES");
				logInfoAt += incAmt;
			}
		}
	}

	const copyFromStream = client.query(
		pgCopyStreams.from(`
	  	COPY osm_datasets.osm_conflation_1_edges(osm, tmc, ris, highway, reversed, from_node, to_node, wkb_geometry)
	  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	logInfo("LOADING", d3intFormat(edges.length),"EDGES INTO DB");

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldEdges()),
			copyFromStream,
			error => {
				if (error) {
					logInfo("STREAM ERROR:", error);
					reject(error);
				}
				else {
					resolve();
				}
			}
		)
	});

	const createGeomIndexSql = `
    CREATE INDEX osm_conflation_1_edges_geom_idx
      ON osm_datasets.osm_conflation_1_edges
      USING GIST(wkb_geometry)
      WITH (fillfactor = 100);
	`;
	await client.query(createGeomIndexSql);

	const createHighwayIndexSql = `
		CREATE INDEX osm_conflation_1_edges_highway_idx
      ON osm_datasets.osm_conflation_1_edges(highway)
      	WITH (fillfactor = 100);
	`;
	await client.query(createHighwayIndexSql);
}

const generateNodesTable = async (client, db) => {
	const createTableSql = `
		CREATE TABLE osm_datasets.osm_conflation_1_nodes(
			ogc_fid BIGSERIAL,
			osm_id BIGINT PRIMARY KEY,
			lon DOUBLE PRECISION,
			lat DOUBLE PRECISION,
			wkb_geometry GEOMETRY(POINT, 4326)
		);
	`;
	await client.query(createTableSql);

/////////////////////////////////////////////////////////
//
// ENSURE ONLY NODES FROM CONFLATED EDGES ARE INCLUDED!!!
//
/////////////////////////////////////////////////////////
	const selectNodesSql = `
		SELECT node_id, lon, lat
			FROM nodes;
	`;
	const nodes = db.all(selectNodesSql);

	const incAmt = 250000;
	let logInfoAt = incAmt;
	let numInserted = 0;

	async function* yieldNodes() {
		for (const { node_id, lon, lat } of nodes) {

			const geojson = JSON.stringify({
				type: "Point",
				coordinates: [+lon, +lat]
			})
			const values = [node_id, +lon, +lat, geojson];

			yield `${ values.join("|") }\n`;

			if (++numInserted >= logInfoAt) {
				logInfo("INSERTED", d3intFormat(numInserted), "NODES");
				logInfoAt += incAmt;
			}
		}
	}

	const copyFromStream = client.query(
		pgCopyStreams.from(`
	  	COPY osm_datasets.osm_conflation_1_nodes(osm_id, lon, lat, wkb_geometry)
	  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	logInfo("LOADING", d3intFormat(nodes.length),"NODES INTO DB");

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldNodes()),
			copyFromStream,
			error => {
				if (error) {
					logInfo("STREAM ERROR:", error);
					reject(error);
				}
				else {
					resolve();
				}
			}
		)
	});

	const createGeomIndexSql = `
    CREATE INDEX osm_conflation_1_nodes_geom_idx
      ON osm_datasets.osm_conflation_1_nodes
      USING GIST(wkb_geometry)
      WITH (fillfactor = 100);
	`;
	await client.query(createGeomIndexSql);
}

const generateMainConflationTable = async (allEdges, client, db) => {

	logInfo("LOADING NODES");
	const allNodes = db.all("SELECT * FROM nodes;");
	logInfo("LOADED", d3intFormat(allNodes.length), "NODES");
	const nodesMap = allNodes.reduce((a, c) => {
		a.set(c.node_id, [+c.lon, +c.lat]);
		return a;
	}, new Map());

	const incAmt = 100000;
	let logInfoAt = incAmt;
	let numInserted = 0;

	const osmEdgegroups = d3groups(allEdges, e => e.way_id, e => e.reversed);

	async function* yieldConflationEdges() {
		for (const [wayId, wayIdGroup] of osmEdgegroups) {
			for (const [reversed, wayEdges] of wayIdGroup) {

				const wayIndexesSet = wayEdges.reduce((a, c) => {
					a.add(+c.way_index);
					return a;
				}, new Set());

				let waybuckets = [...wayIndexesSet].sort((a, b) => a - b)
																						.map(wi => new WayBucket(wi));

				const waybucketsMap = waybuckets.reduce((a, c) => {
					a.set(c.way_index, c);
					return a;
				}, new Map());

				const edgesByRoadId = d3groups(wayEdges, e => e.tmc || e.ris_id)
																	.sort((a, b) => b[1].length - a[1].length);

				for (const [roadId, edges] of edgesByRoadId) {
					for (const edge of edges) {
						waybucketsMap.get(+edge.way_index).pushEdge(edge);
					}
				}

				const completedEdges = [];

				while (waybuckets.length) {
					const [current, ...rest] = waybuckets;

					const edge = current.popEdge();

					if (!edge) {
						waybuckets = rest;
						continue;
					}

					const edges = [edge];

					for (const bucket of rest) {
						const next = bucket.popEdge(edge);
						if (next) {
							edges.push(next);
						}
						else {
							break;
						}
					}
					completedEdges.push(edges);
				}

				for (const edges of completedEdges) {

					const values = edges.reduce((a, c, i) => {

						const { from_node, to_node } = c;
						const fromCoords = nodesMap.get(from_node);
						const toCoords = nodesMap.get(to_node);

						if (i === 0) {
// osm, ris, tmc, year, dir, n, osm_fwd, miles, wkb_geometry
							return [
								c.way_id,
								c.ris_id,
								c.tmc,
								2025,
								null,
								HIGHWAY_TO_F_SYSTEM_MAP[c.highway],
								reversed ? 0 : 1,
								0.0,
								{ type: "LineString",
									coordinates: [fromCoords, toCoords]
								}
							]
						}
						else {
							a[8].coordinates.push(toCoords);
							return a;
						}
					}, []);

					values[7] = turf.length(values[8], { units: "miles" });
					values[8] = JSON.stringify(values[8]);

					yield `${ values.map(v => v === "no-ris" || v === "no-tmc" ? null : v).join("|") }\n`;

					if (++numInserted >= logInfoAt) {
						logInfo("INSERTED", d3intFormat(numInserted), "GEOMETRIES");
						logInfoAt += incAmt;
					}
				}
			}
		}
	}

	const copyFromStream = client.query(
		pgCopyStreams.from(`
	  	COPY osm_datasets.osm_conflation_1(osm, ris, tmc, year, dir, n, osm_fwd, miles, wkb_geometry)
	  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldConflationEdges()),
			copyFromStream,
			error => {
				if (error) {
					logInfo("STREAM ERROR:", error);
					reject(error);
				}
				else {
					resolve();
				}
			}
		)
	});
}