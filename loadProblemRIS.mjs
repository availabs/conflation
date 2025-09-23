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

		DROP TABLE IF EXISTS osm_datasets.problem_ris;

		CREATE TABLE osm_datasets.problem_ris(
			ogc_fid BIGSERIAL PRIMARY KEY,
			ris_id TEXT,
			problems TEXT,
			miles DOUBLE PRECISION,
			wkb_geometry GEOMETRY(MultiLineString, 4326)
		);

		COMMIT;
	`;
	await client.query(createTableSql);
	logInfo("CREATED OUTPUT TABLE: osm_datasets.problem_ris");

	const queryRisSql = `
		SELECT ris_id, ls_index, ris_index, geojson, miles
			FROM ris
			JOIN problem_ris USING(ris_id)
	`
    const ris = db.all(queryRisSql)
                    .map(ris => ({ ...ris,
                                    linestring: JSON.parse(ris.geojson).coordinates
                                })
                    );

    const risIndexSorter = group => group.sort((a, b) => +a.ris_index - +b.ris_index);
    const risRollups = d3rollups(ris, risIndexSorter, r => r.ris_id, r => r.ls_index);

    const fullRisMap = risRollups.reduce((a, c, i) => {
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

    const problemRIS = db.all("SELECT * FROM problem_ris;");
    logInfo("RETRIEVED", d3intFormat(problemRIS.length), "PROBLEM RIS");
    const reducer = group => group.map(g => g.problem).join(", ");
    const problemRisRollups = d3rollups(problemRIS, reducer, p => p.ris_id);

	let incAmt = 500;
	let logInfoAt = incAmt;
	let numRIS = 0;

	logInfo("INSERTING PROBLEM RIS");
	async function* yieldProblemRIS() {
	    for (const [ris_id, problems] of problemRisRollups) {

	    	const geometry = {
	    		type: "MultiLineString",
	    		coordinates: [...fullRisMap[ris_id]]
	    	}
	    	const values = [ris_id, problems, turf.length(geometry, { units: "miles" }), JSON.stringify(geometry)];

	    	if (++numRIS >= logInfoAt) {
	    		logInfo("INSERTED", d3intFormat(numRIS), "RIS ROADWAYS");
	    		logInfoAt += incAmt;
	    	}
	    	yield `${ values.join("|") }\n`;
	    }
 	}

	const copyFromStreamForProblemRIS = client.query(
		pgCopyStreams.from(`
		  	COPY osm_datasets.problem_ris(ris_id, problems, miles, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
		`)
	);

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldProblemRIS()),
			copyFromStreamForProblemRIS,
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
  	await setDamaTables(client, 'Problem RIS', 'gis_dataset', 'osm_datasets.problem_ris');

	db.close();
	await client.end();

	rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()