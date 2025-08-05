import { join, dirname } from "node:path";
import { pipeline, Readable } from "node:stream";
import { fileURLToPath } from 'node:url';
import {
	copyFileSync,
	mkdirSync,
	rmSync
} from "node:fs"

import { format as d3format } from "d3-format"

import * as turf from "@turf/turf";

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import SQLite3DB from "./BetterSQLite3DB.mjs";

import config from "./config.js"
const { npmrds2 } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "saved_checkpoints", "checkpoint-2.sqlite");

const WORKING_DIRECTORY = join(__dirname, "sqlite");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

const d3intFormat = d3format(",d");

const logInfo = (...args) => {
	console.log(
		args.reduce((a, c) => {
		  	if (typeof c === "object") {
		    	return `${ a } ${ JSON.stringify(c) }`;
		  	}
		  	return `${ a } ${ c }`;
		}, `${ new Date().toLocaleString() }:`)
	);
}

(async () => {
    rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
    mkdirSync(WORKING_DIRECTORY);

	logInfo("LOADING SQLITE DB FROM", SQLITE_FILE_PATH);
	copyFileSync(SQLITE_FILE_PATH, ACTIVE_DB_PATH);

	const db = new SQLite3DB(ACTIVE_DB_PATH);

    const client = new pgStuff.Client(npmrds2);
    await client.connect();

// LOAD NODES
    const createNodesTableSql = `
    	DROP TABLE IF EXISTS osm_datasets.edges;
    	DROP TABLE IF EXISTS osm_datasets.nodes;

    	CREATE TABLE osm_datasets.nodes(
    		ogc_fid BIGSERIAL PRIMARY KEY,
    		node_id BIGINT UNIQUE,
    		base_node_id BIGINT,
    		wkb_geometry GEOMETRY(POINT, 4326) 
    	)
    `;
    await client.query(createNodesTableSql);

    const { nodes_count } = db.get("SELECT COUNT(1) AS nodes_count FROM nodes;");
    logInfo("LOADING NODES");

    const queryNodesSql = `
    	SELECT node_id, base_node_id, lon, lat
    		FROM nodes;
    `;
    const nodesIterator = db.prepare(queryNodesSql).iterate();

    const incAmt = 500000;
    let logInfoAt = incAmt;
    let numNodes = 0;

    async function* getNodes() {
	    for (const { node_id, base_node_id, lon, lat } of nodesIterator) {
	    	if (++numNodes >= logInfoAt) {
	    		logInfo("LOADED", d3intFormat(numNodes), "NODES");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ [node_id, base_node_id || '', `POINT(${ lon } ${ lat })`].join("|") }\n`;
	    }
    }

    const copyFromNodesStream = client.query(
    	pgCopyStreams.from(`
        	COPY osm_datasets.nodes(node_id, base_node_id, wkb_geometry)
        		FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '');
      	`)
    );

    logInfo("LOADING", d3intFormat(nodes_count), "NODES");
    await new Promise((resolve, reject) => {
    	pipeline(
    		Readable.from(getNodes()),
    		copyFromNodesStream,
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

	logInfo("CREATING NODES GEOM INDEX");
    const nodesGeomIndexSql = `
    	CREATE INDEX nodes_geom_idx
    		ON osm_datasets.nodes
    		USING GIST(wkb_geometry)
    		WITH (FILLFACTOR = 100)
    `;
    await client.query(nodesGeomIndexSql);

    const nodesTileData = {
    	"type": "circle",
    	"paint": {
    		"circle-radius": 3
    	}
    }
    await setDamaTables("PG Router Nodes", "nodes", client, nodesTileData);

// LOAD EDGES
    const createEdgesTableSql = `
    	CREATE TABLE osm_datasets.edges(
    		ogc_fid BIGSERIAL PRIMARY KEY,
    		source BIGINT NOT NULL,
    		target BIGINT NOT NULL,
    		cost DOUBLE PRECISION NOT NULL,
    		reverse_cost INT NOT NULL DEFAULT -1,
    		wkb_geometry GEOMETRY(LINESTRING, 4326),
	        FOREIGN KEY(source) REFERENCES osm_datasets.nodes(node_id) ON DELETE CASCADE,
	        FOREIGN KEY(target) REFERENCES osm_datasets.nodes(node_id) ON DELETE CASCADE
    	);
    `;
    await client.query(createEdgesTableSql);

    const { edges_count } = db.get("SELECT COUNT(1) AS edges_count FROM edges;");

    const queryEdgesSql = `
    	SELECT from_node, to_node, length
    		FROM edges;
    `;
    const edgesIterator = db.prepare(queryEdgesSql).iterate();

    logInfoAt = incAmt;
    let numEdges = 0;

    const selectNodesSql = `
    	SELECT node_id, lon, lat
    		FROM nodes
    		WHERE node_id in (?, ?);
    `;
    const selectNodesStmt = db.prepare(selectNodesSql);

    async function* getEdges() {
	    for (const { from_node, to_node, length } of edgesIterator) {
	    	if (++numEdges >= logInfoAt) {
	    		logInfo("LOADED", d3intFormat(numEdges), "EDGES");
	    		logInfoAt += incAmt;
	    	}
	    	const nodes = selectNodesStmt.all(from_node, to_node)
	    		.reduce((a, c) => {
	    			a[c.node_id] = `${ c.lon } ${ c.lat }`;
	    			return a;
	    		}, {});
	    	const linestring = `${ nodes[from_node] }, ${ nodes[to_node] }`;
	    	yield `${ [from_node, to_node, length, `LINESTRING(${ linestring })`].join("|") }\n`;
	    }
    }

    const copyFromEdgesStream = client.query(
    	pgCopyStreams.from(`
        	COPY osm_datasets.edges(source, target, cost, wkb_geometry)
        		FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '');
      	`)
    );

    logInfo("LOADING", d3intFormat(edges_count), "EDGES");
    await new Promise((resolve, reject) => {
    	pipeline(
    		Readable.from(getEdges()),
    		copyFromEdgesStream,
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

	logInfo("CREATING EDGES GEOM INDEX");
    const edgesGeomIndexSql = `
    	CREATE INDEX edges_geom_idx
    		ON osm_datasets.edges
    		USING GIST(wkb_geometry)
    		WITH (FILLFACTOR = 100)
    `;
    await client.query(edgesGeomIndexSql);

    const edgesTileData = {
    	"type": "line",
    	"paint": {
    		"line-width": 3,
    		"line-offset": 3
    	}
    }
    await setDamaTables("PG Router Edges", "edges", client, edgesTileData);

// RELEASE RESOURCES
	db.close();
	await client.end();

    rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()

const setDamaTables = async (sourceName, tableName, client, tileData) => {
	logInfo("GENERATING DAMA TABLES");

	const deleteViewSql = `
		DELETE FROM data_manager.views
			WHERE source_id IN (
				SELECT source_id
					FROM data_manager.sources
					WHERE name = $1
			);
	`;
	await client.query(deleteViewSql, [sourceName]);

	const deleteSourceSql = `
		DELETE FROM data_manager.sources
			WHERE name = $1;
	`;
	await client.query(deleteSourceSql, [sourceName]);

	const insertSourceSql = `
		INSERT INTO data_manager.sources(name, type)
			VALUES($1, 'gis_dataset')
		RETURNING source_id;
	`;
	const { rows: [{ source_id }] } = await client.query(insertSourceSql, [sourceName]);
	logInfo("GOT NEW SOURCE ID:", source_id);

	const dataTable = `osm_datasets.${ tableName }`;

	const insertViewSql = `
		INSERT INTO data_manager.views(source_id, data_type, table_schema, table_name, data_table)
			VALUES(${ source_id }, 'gis_dataset', 'osm_datasets', $1, $2)
		RETURNING view_id;
	`;
	const { rows: [{ view_id }] } = await client.query(insertViewSql, [tableName, dataTable]);
	logInfo("GOT NEW VIEW ID:", view_id);

	const statisticsSql = `
		UPDATE data_manager.sources
			SET statistics = '{"auth": {"users": {}, "groups": {"AVAIL": "10", "public": "2"}}}'
			WHERE source_id = $1;
	`;
	await client.query(statisticsSql, [source_id]);

	const initMetadataSql = `
		CALL _data_manager_admin.initialize_dama_src_metadata_using_view($1)
	`;
	await client.query(initMetadataSql, [view_id]);

  	const tilesName = `npmrds2_s${ source_id }_v${ view_id }_${ Date.now() }`;
  	const tilesMetadata = {
  		tiles: {
	  		"sources": [
	  			{	"id": tilesName,
	  				"source": {
	  					"tiles": [`https://graph.availabs.org/dama-admin/npmrds2/tiles/${ view_id }/{z}/{x}/{y}/t.pbf`],
	  					"format": "pbf",
	  					"type": "vector"
	  				}
	  			}
	  		],
	  		"layers": [
	  			{	"id": `${ source_id }_v${ view_id }_polygons`,
	  				"source": tilesName,
	  				"source-layer": `view_${ view_id }`,
	  				...tileData
	  			}
	  		]
	  	}
  	};

  	const updateMetadataWithTilesSql = `
  		UPDATE data_manager.views
  			SET metadata = COALESCE(metadata, '{}') || '${ JSON.stringify(tilesMetadata) }'::JSONB
  			WHERE view_id = $1;
  	`;
  	await client.query(updateMetadataWithTilesSql, [view_id]);
}