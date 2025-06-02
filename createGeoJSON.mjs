import { join, dirname } from "node:path";
import { pipeline, Readable } from "node:stream";
import { fileURLToPath } from 'node:url';
import { createWriteStream } from "node:fs"

import { format as d3format } from "d3-format"
import { group as d3group, rollups as d3rollups } from "d3-array"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import SQLite3DB from "./BetterSQLite3DB.mjs";

import config from "./config.js"
const { npmrds2 } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const sqliteFilePath = join(__dirname, "checkpoints", "checkpoint-1.sqlite");

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
	logInfo("LOADING SQLITE DB FROM", sqliteFilePath);
	const db = new SQLite3DB(sqliteFilePath);

	logInfo("QUERYING NODES");
	const nodes = db.all("SELECT * FROM nodes;");
	logInfo("RECIEVED", d3intFormat(nodes.length), "NODES");
	const nodesMap = nodes.reduce((a, c) => { a.set(c.node_id, c); return a; }, new Map());

	logInfo("QUERYING EDGES");
	const edges = db.all("SELECT * FROM edges;");
	logInfo("RECIEVED", d3intFormat(edges.length), "EDGES");

    const sorter = way => way.sort((a, b) => +a.pos - +b.pos);
    const rollups = d3rollups(edges, sorter, e => e.way_id, e => e.reversed);

	const collection = {
		type: "FeatureCollection",
		features: []
	}

    const client = new pgStuff.Client(npmrds2);
    await client.connect();

    const createTableSql = `
    	BEGIN;

    	DROP TABLE IF EXISTS osm_datasets.checkpoint_1_test;

    	CREATE TABLE osm_datasets.checkpoint_1_test(
    		ogc_fid BIGSERIAL PRIMARY KEY,
    		way_id BIGINT,
    		highway TEXT,
    		reversed BOOLEAN,
    		wkb_geometry GEOMETRY(LINESTRING, 4032)
    	);

    	COMMIT;
    `;
    await client.query(createTableSql);
    logInfo("CREATED TABLE");

    const insertWaySql = `
    	INSERT INTO osm_datasets.checkpoint_1_test(way_id, highway, reversed, wkb_geometry)
    		VALUES($1, $2, $3, $4)
    `;

    const logIncAmt = 100000;
    let logInfoAt = logIncAmt;
    let numWays = 0;

    logInfo("PROCESSING WAYS");

    async function* processWays() {
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
	    			way_id,
	    			highway,
	    			Boolean(reversed),
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

    const copyFromStream = client.query(
    	pgCopyStreams.from(`
        	COPY osm_datasets.checkpoint_1_test(way_id, highway, reversed, wkb_geometry)
        	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|')
      	`)
    );

    await new Promise((resolve, reject) => {
    	pipeline(
    		Readable.from(processWays()),
    		copyFromStream,
    		error => {
    			if (error) {
    				logInfo("STREAM ERROR:", error);
    				reject();
    			}
    			else {
    				resolve();
    			}
    		}
    	)
    }).catch(error => {
    	throw new Error(error.message || error);
    })
	
	logInfo("PROCESSED", d3intFormat(numWays), "TOTAL WAYS");

	db.close();
	await client.end();
})()