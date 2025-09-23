import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';
import { pipeline, Readable } from "node:stream";
import {
	copyFileSync,
	mkdirSync,
	rmSync
} from "node:fs"

import pgStuff from "pg";
// import pgCopyStreams from "pg-copy-streams";

import { format as d3format } from "d3-format"
import { groups as d3groups } from "d3-array"

import SQLite3DB from "./BetterSQLite3DB.mjs";

import setDamaTables from "./setDamaTables.mjs";

import config from "./config.js"
const { db_info } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "TMC_checkpoints", "checkpoint-3.sqlite");

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

const loadConflation = async options => {
  rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
  mkdirSync(WORKING_DIRECTORY);

	const {
		SQLITE_FILE_PATH,
		createConflationTableSql,
		queryConflationEdgesSql,
		makeCopyFromStream,
		incAmt = 10000,
		damaArgs
	} = options;

	logInfo("LOADING SQLITE DB FROM", SQLITE_FILE_PATH);
	copyFileSync(SQLITE_FILE_PATH, ACTIVE_DB_PATH);

	const db = new SQLite3DB(ACTIVE_DB_PATH);

	logInfo("CONNECTING CLIENT");
  const client = new pgStuff.Client(db_info);
  await client.connect();
	logInfo("CLIENT CONNECTED");

	await client.query(createConflationTableSql);
	logInfo("CREATED CONFLATION TABLE:", damaArgs[2]);

	logInfo("QUERYING CONFLATION EDGES");
	const conflationEdges = db.all(queryConflationEdgesSql);

	const roadGroups = d3groups(conflationEdges, e => e.road_id, e => e.way_id);

	let logInfoAt = incAmt;
	let numInserts = 0;

	logInfo("INSERTING CONFLATION GEOMETRIES");
	async function* yieldConflationEdges() {
		for (const [road_id, wayIdGroup] of roadGroups) {
			for (const [way_id, edges] of wayIdGroup) {

				const coordinates = edges.sort((a, b) => a.road_index - b.road_index)
					.map(e => JSON.parse(e.edge))
					.reduce((a, c, i) => {
						if (i === 0) {
							a.push(...c);
						}
						else {
							a.push(c[1]);
						}
						return a;
					}, [])

				const wkb_geometry = JSON.stringify({
					type: "LineString",
					coordinates
				});
				const values = [way_id, road_id, wkb_geometry]

    	if (++numInserts >= logInfoAt) {
    		logInfo("INSERTED", d3intFormat(numInserts), "GEOMETRIES");
    		logInfoAt += incAmt;
    	}
    	yield `${ values.join("|") }\n`;
			}
		}
 	}

	await new Promise((resolve, reject) => {
		pipeline(
			Readable.from(yieldConflationEdges()),
			makeCopyFromStream(client),
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
	await setDamaTables(client, ...damaArgs);

	db.close();
	await client.end();

  rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
}
export default loadConflation;