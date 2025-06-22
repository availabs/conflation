import { join, dirname } from "node:path";
import { pipeline, Readable } from "node:stream";
import { fileURLToPath } from 'node:url';
import {
	copyFileSync,
	mkdirSync,
	rmSync
} from "node:fs"

import { format as d3format } from "d3-format"
import { group as d3group, rollups as d3rollups } from "d3-array"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import SQLite3DB from "./BetterSQLite3DB.mjs";

import config from "./config.js"
const { npmrds2 } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "checkpoints", "checkpoint-1.sqlite");

const WORKING_DIRECTORY = join(__dirname, "sqlite");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

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

(async () => {
    rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
    mkdirSync(WORKING_DIRECTORY);

	logInfo("LOADING SQLITE DB FROM", SQLITE_FILE_PATH);
	copyFileSync(SQLITE_FILE_PATH, ACTIVE_DB_PATH);
	const db = new SQLite3DB(ACTIVE_DB_PATH);

	const createWayIdsTableSql = `
		CREATE TABLE way_ids_table
			AS
				SELECT DISTINCT way_id
					FROM edges
	`;
	db.run(createWayIdsTableSql);

    const wayIdCountSql = `
    	SELECT COUNT(way_id) AS way_id_count
    		FROM way_ids_table
    `;
    const { way_id_count } = await db.get(wayIdCountSql);
    logInfo("WAY ID COUNT:", d3intFormat(way_id_count));

    const client = new pgStuff.Client(npmrds2);
    await client.connect();

    const createTableSql = `
    	BEGIN;

    	DROP TABLE IF EXISTS osm_datasets.checkpoint_1_test;

    	CREATE TABLE osm_datasets.checkpoint_1_test(
    		ogc_fid BIGSERIAL PRIMARY KEY,
    		way_id BIGINT,
    		highway TEXT,
    		reversed INT,
    		wkb_geometry GEOMETRY(LINESTRING, 4032)
    	);

    	COMMIT;
    `;
    await client.query(createTableSql);
    logInfo("CREATED OUTPUT TABLE: osm_datasets.checkpoint_1_test");

    const wayIdLimit = 50000;

    const logIncAmt = 50000;
    let logInfoAt = logIncAmt;
    let numWays = 0;

    async function* processWays() {
    	for (let wayIdOffset = 0; wayIdOffset < way_id_count; wayIdOffset += wayIdLimit) {

    		logInfo("PROCESSING WAYS", d3intFormat(wayIdOffset), "TO", d3intFormat(wayIdOffset + wayIdLimit));

    		const queryEdgesSql = `
    			WITH way_ids AS(
	    				SELECT way_id
							FROM way_ids_table
	    					LIMIT ${ wayIdLimit }
	    					OFFSET ${ wayIdOffset }
	    			)
    			SELECT *
    				FROM edges
    				JOIN way_ids
    					USING(way_id)
    		`;

			logInfo("QUERYING SQLITE DB FOR EDGES");
			const edges = db.all(queryEdgesSql);
			logInfo("RECIEVED", d3intFormat(edges.length), "EDGES");

		    const sorter = way => way.sort((a, b) => +a.pos - +b.pos);
		    const rollups = d3rollups(edges, sorter, e => e.way_id, e => e.reversed);

    		const queryNodesSql = `
    			WITH way_ids AS(
	    				SELECT way_id
							FROM way_ids_table
	    					LIMIT ${ wayIdLimit }
	    					OFFSET ${ wayIdOffset }
	    			),
    				all_node_ids AS(
    					SELECT DISTINCT to_node AS node_id
    						FROM edges
    							JOIN way_ids
    								ON edges.way_id = way_ids.way_id

    					UNION

    					SELECT DISTINCT from_node AS node_id
    						FROM edges
    							JOIN way_ids
    								ON edges.way_id = way_ids.way_id
    				),
    				distinct_node_ids AS(
    					SELECT DISTINCT(node_id)
    						FROM all_node_ids
    				)
    			SELECT *
    				FROM nodes
    					JOIN distinct_node_ids
    						USING(node_id)

    		`;

			logInfo("QUERYING SQLITE DB FOR NODES");
			const nodes = db.all(queryNodesSql);
			logInfo("RECIEVED", d3intFormat(nodes.length), "NODES");
			const nodesMap = nodes.reduce((a, c) => { a.set(c.node_id, c); return a; }, new Map());

		    for (const [way_id, ways] of rollups) {
		    	for (const [reversed, way] of ways) {
		    		const linestring = way.reduce((a, c, i) => {
		    			const { from_node, to_node } = c;
		    			if (i === 0) {
		    				const from = nodesMap.get(from_node);
		    				const to = nodesMap.get(to_node);
		    				a.push(`${ from.lon } ${ from.lat }`, `${ to.lon } ${ to.lat }`);
		    			}
		    			else {
		    				const to = nodesMap.get(to_node);
		    				a.push(`${ to.lon } ${ to.lat }`);
		    			}
		    			return a;
		    		}, []);
		    		const highwayMap = way.reduce((a, c) => {
		    			const { highway } = c;
		    			if (!(highway in c)) {
		    				a[highway] = 0;
		    			}
		    			a[highway] += 1;
		    			return a;
		    		}, {});
		    		const [[highway]] = Object.entries(highwayMap).sort((a, b) => b[1] - a[1]);

		    		const values = [
		    			way_id, highway, reversed,
		    			`LINESTRING(${ linestring })`
		    		];

		    		if (++numWays >= logInfoAt) {
		    			logInfo("PROCESSED", d3intFormat(numWays), "WAYS");
		    			logInfoAt += logIncAmt;
		    		}

		    		yield `${ values.join("|") }\n`;
		    	}
		    }
    	}
    }

    const copyFromStream = client.query(
    	pgCopyStreams.from(`
        	COPY osm_datasets.checkpoint_1_test(way_id, highway, reversed, wkb_geometry)
        	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
      	`)
    );

    await new Promise((resolve, reject) => {
    	pipeline(
    		Readable.from(processWays()),
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
	
	logInfo("PROCESSED", d3intFormat(numWays), "TOTAL WAYS");

// SET DAMA TABLES
	const deleteViewSql = `
		DELETE FROM data_manager.views
			WHERE source_id IN (
				SELECT source_id
					FROM data_manager.sources
					WHERE name = 'Checkpoint 1 Test'
			);
	`;
	await client.query(deleteViewSql);

	const deleteSourceSql = `
		DELETE FROM data_manager.sources
			WHERE name = 'Checkpoint 1 Test';
	`;
	await client.query(deleteSourceSql);

	const insertSourceSql = `
		INSERT INTO data_manager.sources(name, type)
			VALUES('Checkpoint 1 Test', 'gis_dataset')
		RETURNING source_id;
	`;
	const { rows: [{ source_id }] } = await client.query(insertSourceSql);
	logInfo("GOT NEW SOURCE ID:", source_id);

	const insertViewSql = `
		INSERT INTO data_manager.views(source_id, data_type, table_schema, table_name, data_table)
			VALUES(${ source_id }, 'gis_dataset', 'osm_datasets', 'checkpoint_1_test', 'osm_datasets.checkpoint_1_test')
		RETURNING view_id;
	`;
	const { rows: [{ view_id }] } = await client.query(insertViewSql);
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
	  				"type": "line",
	  				"source": tilesName,
	  				"source-layer": `view_${ view_id }`,
	  				"paint": {
	  					"line-offset": 1.25
	  				}
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

	db.close();
	await client.end();

    rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()