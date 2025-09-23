import { join, dirname } from "node:path";
import { pipeline, Readable } from "node:stream";
import { fileURLToPath } from 'node:url';
import {
	copyFileSync,
	mkdirSync,
	rmSync
} from "node:fs"

import { format as d3format } from "d3-format"
import { rollups as d3rollups } from "d3-array"

import * as turf from "@turf/turf";

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import SQLite3DB from "./BetterSQLite3DB.mjs";

import setDamaTables from "./setDamaTables.mjs";

import config from "./config.js"
const { db_info } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "TMC_checkpoints", "checkpoint-3.sqlite");

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
	const client = new pgStuff.Client(db_info);
	await client.connect();
	logInfo("CLIENT CONNECTED");

	const createTableSql = `
		BEGIN;

		DROP TABLE IF EXISTS osm_datasets.npmrds_checkpoint_3_test;

		CREATE TABLE osm_datasets.npmrds_checkpoint_3_test(
			ogc_fid BIGSERIAL PRIMARY KEY,
			tmc TEXT,
			miles DOUBLE PRECISION,
			result_type TEXT,
			wkb_geometry GEOMETRY(MultiLineString, 4032)
		);

		COMMIT;
	`;
	await client.query(createTableSql);
	logInfo("CREATED OUTPUT TABLE: osm_datasets.npmrds_checkpoint_3_test");

	const finalResultsIterator = db.prepare("SELECT * FROM final_tmc_results;").iterate();

	let incAmt = 5000;
	let logInfoAt = incAmt;
	let numResults = 0;

	logInfo("INSERTING FINAL RESULTS");
	async function* yieldFinalResults() {
	    for (const result of finalResultsIterator) {

	    	const { tmc, miles, wkb_geometry } = result;

    		const values = [tmc, miles, 'final-result', wkb_geometry];

	    	if (++numResults >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numResults), "RESULTS");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ values.join("|") }\n`;
	    }
 	}

	const copyFromStreamForFinalResults = client.query(
		pgCopyStreams.from(`
		  	COPY osm_datasets.npmrds_checkpoint_3_test(tmc, miles, result_type, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldFinalResults()),
			copyFromStreamForFinalResults,
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

	const queryTMCsSql = `
		SELECT tmc, ls_index, tmc_index, geojson, miles
			FROM tmcs
			WHERE tmc IN (
				SELECT DISTINCT tmc FROM final_tmc_results
			)
	`;
    const tmcs = db.all(queryTMCsSql)
                    .map(tmc => ({ ...tmc,
                                    linestring: JSON.parse(tmc.geojson).coordinates
                                })
                    );

    const tmcIndexSorter = group => group.sort((a, b) => +a.tmc_index - +b.tmc_index);
    const tmcRollups = d3rollups(tmcs, tmcIndexSorter, r => r.tmc, r => r.ls_index);

    const fullTMCsMap = tmcRollups.reduce((a, c, i) => {
    	const [tmc, [[lsIndex, segments]]] = c;
    	if (!(tmc in a)) {
    		a[tmc] = [];
    	}
    	const linestring = segments.reduce((aa, cc, ii) => {
    		if (ii === 0) {
    			return [...cc.linestring];
    		}
    		else {
    			return [
    				...aa,
    				...cc.linestring.slice(1)
    			]
    		}
    	}, []);
    	a[tmc].push(linestring);
    	return a;
    }, {});

	logInfoAt = incAmt;
	numResults = 0;

	logInfo("INSERTING FINAL FULL TMCs");
    async function* yieldTMCs() {
	    for (const [tmc, linestrings] of Object.entries(fullTMCsMap)) {

	    	const geometry = {
	    		type: "MultiLineString",
	    		coordinates: linestrings
	    	}
	    	const values = [tmc, turf.length(geometry, { units: "miles" }), "tmc-base", JSON.stringify(geometry)];

	    	if (++numResults >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numResults), "TMCs");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ values.join("|") }\n`;
	    }
    }

	const copyFromStreamForTMCs = client.query(
		pgCopyStreams.from(`
		  	COPY osm_datasets.npmrds_checkpoint_3_test(tmc, miles, result_type, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldTMCs()),
			copyFromStreamForTMCs,
			error => {
				if (error) {
					logInfo("STREAM ERROR:", error);
					logInfo("CURRENT:", JSON.stringify(current, null, 3));
					reject(error);
				}
				else {
					resolve();
				}
			}
		)
	});

// SET DAMA TABLES
// ARGS: client, name, data_type, data_table
  	await setDamaTables(client, 'NPMRDS Checkpoint 3 Test', 'gis_dataset', 'osm_datasets.npmrds_checkpoint_3_test');

	db.close();
	await client.end();

	rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()