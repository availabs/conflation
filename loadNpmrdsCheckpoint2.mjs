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

import setDamaTables from "./setDamaTables.mjs";

import config from "./config.js"
const { db_info } = config;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "TMC_checkpoints", "checkpoint-2.sqlite");

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

    DROP TABLE IF EXISTS osm_datasets.npmrds_checkpoint_2_test;

    CREATE TABLE osm_datasets.npmrds_checkpoint_2_test(
      ogc_fid BIGSERIAL PRIMARY KEY,
      tmc TEXT,
      ls_index SMALLINT,
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
  logInfo("CREATED OUTPUT TABLE: osm_datasets.npmrds_checkpoint_2_test");

  const queryResultsSql = "SELECT * FROM tmc_results;";
  const resultsIterator = db.prepare(queryResultsSql).iterate();

  let incAmt = 50000;
  let logInfoAt = incAmt;
  let numResults = 0;

  logInfo("INSERTING RESULTS");
  async function* insertResults() {
    for (const result of resultsIterator) {

      const {
        tmc,
        ls_index,
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
          ls_index,
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
        COPY osm_datasets.npmrds_checkpoint_2_test(tmc, ls_index, tmc_index, result_type, rank, start_score, end_score, miles_score, miles, wkb_geometry)
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
    SELECT tmc, ls_index, tmc_index, geojson, miles
      FROM tmcs
      WHERE tmc IN (
        SELECT DISTINCT tmc FROM tmc_results
      )
  `;
  const processedTMCsIterator = db.prepare(queryProcessedTMCsSql).iterate();
  
  incAmt = 25000;
  logInfoAt = incAmt;
  let numTMCs = 0;

  logInfo("INSERTING TMCs");
  async function* insertTMCs() {
    for (const { tmc, ls_index, tmc_index, miles, geojson } of processedTMCsIterator) {

      if (++numTMCs >= logInfoAt) {
        logInfo("INSERTED", d3intFormat(numTMCs), "TMC SEGMENTS");
        logInfoAt += incAmt;
      }
      const values = [tmc, ls_index, tmc_index, "tmc-base", 0, miles, geojson];

      yield `${ values.join("|") }\n`;
    }
  }

  const copyFromStreamForTMCs = client.query(
    pgCopyStreams.from(`
        COPY osm_datasets.npmrds_checkpoint_2_test(tmc, ls_index, tmc_index, result_type, rank, miles, wkb_geometry)
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
// ARGS: client, name, data_type, data_table
  await setDamaTables(client, 'NPMRDS Checkpoint 2 Test', 'gis_dataset', 'osm_datasets.npmrds_checkpoint_2_test');

  db.close();
  await client.end();

  rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
})()