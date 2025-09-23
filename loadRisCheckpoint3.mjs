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

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "RIS_checkpoints", "checkpoint-3.sqlite");

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

		DROP TABLE IF EXISTS osm_datasets.ris_checkpoint_3_test;

		CREATE TABLE osm_datasets.ris_checkpoint_3_test(
			ogc_fid BIGSERIAL PRIMARY KEY,
			ris_id TEXT,
			miles DOUBLE PRECISION,
			result_type TEXT,
			wkb_geometry GEOMETRY(MultiLineString, 4032)
		);

		COMMIT;
	`;
	await client.query(createTableSql);
	logInfo("CREATED OUTPUT TABLE: osm_datasets.ris_checkpoint_3_test");

	const finalResultsIterator = db.prepare("SELECT * FROM final_ris_results;").iterate();

	let incAmt = 25000;
	let logInfoAt = incAmt;
	let numResults = 0;

	logInfo("INSERTING FINAL RESULTS");
	async function* yieldFinalResults() {
	    for (const result of finalResultsIterator) {

	    	const { ris_id, miles, wkb_geometry } = result;

    		const values = [ris_id, miles, 'final-result', wkb_geometry];

	    	if (++numResults >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numResults), "RESULTS");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ values.join("|") }\n`;
	    }
 	}

	const copyFromStreamForFinalResults = client.query(
		pgCopyStreams.from(`
		  	COPY osm_datasets.ris_checkpoint_3_test(ris_id, miles, result_type, wkb_geometry)
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

	const queryRisSql = `
		SELECT ris_id, ls_index, ris_index, geojson, miles
			FROM ris
			WHERE ris_id IN (
				SELECT DISTINCT ris_id FROM final_ris_results
			)
	`;
    const ris = db.all(queryRisSql)
                    .map(ris => ({ ...ris,
                                    linestring: JSON.parse(ris.geojson).coordinates
                                })
                    );

    const risIndexSorter = group => group.sort((a, b) => +a.ris_index - +b.ris_index);
    const risRollups = d3rollups(ris, risIndexSorter, r => r.ris_id, r => r.ls_index);

    const fullRISMap = risRollups.reduce((a, c, i) => {
    	const [ris_id, [[lsIndex, segments]]] = c;
    	if (!(ris_id in a)) {
    		a[ris_id] = [];
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
    	a[ris_id].push(linestring);
    	return a;
    }, {});

	logInfoAt = incAmt;
	numResults = 0;

	logInfo("INSERTING FINAL FULL RIS");
    async function* yieldRIS() {
	    for (const [ris_id, linestrings] of Object.entries(fullRISMap)) {

	    	const geometry = {
	    		type: "MultiLineString",
	    		coordinates: linestrings
	    	}
	    	const values = [ris_id, turf.length(geometry, { units: "miles" }), "ris-base", JSON.stringify(geometry)];

	    	if (++numResults >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numResults), "RIS ROADWAYS");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ values.join("|") }\n`;
	    }
    }

	const copyFromStreamForTMCs = client.query(
		pgCopyStreams.from(`
		  	COPY osm_datasets.ris_checkpoint_3_test(ris_id, miles, result_type, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldRIS()),
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
  	await setDamaTables(client, 'RIS Checkpoint 3 Test', 'gis_dataset', 'osm_datasets.ris_checkpoint_3_test');

	db.close();
	await client.end();

	rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()