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

	logInfo("CONNECTING CLIENT");
  const client = new pgStuff.Client(npmrds2);
  await client.connect();
	logInfo("CLIENT CONNECTED");

  const createTableSql = `
  	BEGIN;

  	DROP TABLE IF EXISTS osm_datasets.checkpoint_2_test;

  	CREATE TABLE osm_datasets.checkpoint_2_test(
  		ogc_fid BIGSERIAL PRIMARY KEY,
  		tmc TEXT,
  		linestring_index SMALLINT,
  		tmc_index SMALLINT,
  		result_type TEXT,
  		rank INT,
  		start_score DOUBLE PRECISION,
  		end_score DOUBLE PRECISION,
  		miles_score DOUBLE PRECISION,
  		miles DOUBLE PRECISION,
  		wkb_geometry GEOMETRY(GEOMETRY, 4032)
  	);

  	COMMIT;
  `;
  await client.query(createTableSql);
  logInfo("CREATED OUTPUT TABLE: osm_datasets.checkpoint_2_test");

  const queryResultsSql = "SELECT * FROM results ORDER BY tmc;";
  const resultsIterator = db.prepare(queryResultsSql).iterate();

  let incAmt = 10000;
  let logInfoAt = incAmt;
  let numResults = 0;

  logInfo("INSERTING RESULTS");
  async function* insertResults() {
    for (const result of resultsIterator) {

    	const {
    		tmc,
    		linestring_index,
    		tmc_index,
    		path,
    		rank,
    		start_score,
    		end_score,
    		miles_score,
    		miles
    	} = result;

    	const linestring = JSON.parse(path).map(n => `${ n.lon } ${ n.lat }`);

    	if (linestring.length > 1) {
    		const values = [
    			tmc,
    			linestring_index,
    			tmc_index,
    			"result",
    			rank,
    			start_score,
    			end_score,
    			miles_score,
    			miles,
    			`LINESTRING(${ linestring })`
    		];

	    	if (++numResults >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numResults), "RESULTS");
	    		logInfoAt += incAmt;
	    	}

	    	yield `${ values.join("|") }\n`;
    	}
    }
  }

  const copyFromStreamForResults = client.query(
  	pgCopyStreams.from(`
      	COPY osm_datasets.checkpoint_2_test(tmc, linestring_index, tmc_index, result_type, rank, start_score, end_score, miles_score, miles, wkb_geometry)
      	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
    	`)
  );

  await new Promise((resolve, reject) => {
  	pipeline(
  		Readable.from(insertResults()),
  		copyFromStreamForResults,
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

  const queryProcessedTMCsSql = `
  	SELECT tmc, linestring_index, tmc_index, geojson, miles
  		FROM tmcs
  		WHERE tmc IN (
  			SELECT DISTINCT tmc FROM results
  		)
  `;
  const processedTMCsIterator = db.prepare(queryProcessedTMCsSql).iterate();

	incAmt = 2500;
  logInfoAt = incAmt;
  let numTMCs = 0;

  logInfo("INSERTING TMCs");
  async function* insertTMCs() {
    for (const { tmc, linestring_index, tmc_index, miles, geojson } of processedTMCsIterator) {

    	if (++numTMCs >= logInfoAt) {
    		logInfo("INSERTED", d3intFormat(numTMCs), "TMC SEGMENTS");
    		logInfoAt += incAmt;
    	}
    	const values = [tmc, linestring_index, tmc_index, "tmc-base", 0, miles, geojson];

    	yield `${ values.join("|") }\n`;
    }
  }

  const copyFromStreamForTMCs = client.query(
  	pgCopyStreams.from(`
      	COPY osm_datasets.checkpoint_2_test(tmc, linestring_index, tmc_index, result_type, rank, miles, wkb_geometry)
      	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
    	`)
  );

  await new Promise((resolve, reject) => {
  	pipeline(
  		Readable.from(insertTMCs()),
  		copyFromStreamForTMCs,
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

// SET DAMA TABLES
	const deleteViewSql = `
		DELETE FROM data_manager.views
			WHERE source_id IN (
				SELECT source_id
					FROM data_manager.sources
					WHERE name = 'Checkpoint 2 Test'
			);
	`;
	await client.query(deleteViewSql);

	const deleteSourceSql = `
		DELETE FROM data_manager.sources
			WHERE name = 'Checkpoint 2 Test';
	`;
	await client.query(deleteSourceSql);

	const insertSourceSql = `
		INSERT INTO data_manager.sources(name, type)
			VALUES('Checkpoint 2 Test', 'gis_dataset')
		RETURNING source_id;
	`;
	const { rows: [{ source_id }] } = await client.query(insertSourceSql);
	logInfo("GOT NEW SOURCE ID:", source_id);

	const insertViewSql = `
		INSERT INTO data_manager.views(source_id, data_type, table_schema, table_name, data_table)
			VALUES(${ source_id }, 'gis_dataset', 'osm_datasets', 'checkpoint_2_test', 'osm_datasets.checkpoint_2_test')
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

  	const tilesName = `npmrds2_s${ source_id }_v${ view_id }`;
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