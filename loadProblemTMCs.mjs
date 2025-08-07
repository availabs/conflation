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
const { npmrds2 } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "saved_checkpoints", "checkpoint-3.sqlite");

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

		DROP TABLE IF EXISTS osm_datasets.problem_tmcs;

		CREATE TABLE osm_datasets.problem_tmcs(
			ogc_fid BIGSERIAL PRIMARY KEY,
			tmc TEXT,
			problems TEXT,
			miles DOUBLE PRECISION,
			wkb_geometry GEOMETRY(MultiLineString, 4326)
		);

		COMMIT;
	`;
	await client.query(createTableSql);
	logInfo("CREATED OUTPUT TABLE: osm_datasets.problem_tmcs");

	const queryTMCsSql = `
		SELECT tmc, linestring_index, tmc_index, geojson, miles
			FROM tmcs
			JOIN problem_tmcs USING(tmc)
	`
    const tmcs = db.all(queryTMCsSql)
                    .map(tmc => ({ ...tmc,
                                    linestring: JSON.parse(tmc.geojson).coordinates
                                })
                    );

    const tmcIndexSorter = group => group.sort((a, b) => +a.tmc_index - +b.tmc_index);
    const tmcRollups = d3rollups(tmcs, tmcIndexSorter, r => r.tmc, r => r.linestring_index);

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

    const problemTMCs = db.all("SELECT * FROM problem_tmcs;");
    logInfo("RETRIEVED", d3intFormat(problemTMCs.length), "PROBLEM TMCs");
    const reducer = group => group.map(g => g.problem).join(", ");
    const problemTmcRollups = d3rollups(problemTMCs, reducer, p => p.tmc);

	let incAmt = 500;
	let logInfoAt = incAmt;
	let numTMCs = 0;

	logInfo("INSERTING PROBLEM TMCs");
	async function* yieldProblemTMCs() {
	    for (const [tmc, problems] of problemTmcRollups) {

	    	const geometry = {
	    		type: "MultiLineString",
	    		coordinates: [...fullTMCsMap[tmc]]
	    	}
	    	const values = [tmc, problems, turf.length(geometry, { units: "miles" }), JSON.stringify(geometry)];

	    	if (++numTMCs >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numTMCs), "TMCs");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ values.join("|") }\n`;
	    }
 	}

	const copyFromStreamForProblemTMCs = client.query(
		pgCopyStreams.from(`
		  	COPY osm_datasets.problem_tmcs(tmc, problems, miles, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldProblemTMCs()),
			copyFromStreamForProblemTMCs,
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
// ARGS: client, name, data_type, data_table
  	await setDamaTables(client, 'Problem TMCs', 'gis_dataset', 'osm_datasets.problem_tmcs');

	db.close();
	await client.end();

	rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()