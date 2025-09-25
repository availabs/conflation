import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';

import pgCopyStreams from "pg-copy-streams";

import loadConflation from "./loadConflation.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "checkpoints", "checkpoint-4.sqlite");

(async () => {
	const createConflationTableSql = `
		BEGIN;

		DROP TABLE IF EXISTS osm_datasets.npmrds_conflation;

		CREATE TABLE osm_datasets.npmrds_conflation(
			ogc_fid BIGSERIAL PRIMARY KEY,
			way_id BIGINT,
			tmc TEXT,
			wkb_geometry GEOMETRY(LineString, 4032)
		);

		COMMIT;
	`;

	const queryConflationEdgesSql = `
		SELECT
				way_id,
				way_index,
				tmc AS road_id,
				tmc_index AS road_index,
				edge
			FROM npmrds_conflation;
	`;

	const makeCopyFromStream = client => {
		return client.query(
			pgCopyStreams.from(`
		  	COPY osm_datasets.npmrds_conflation(way_id, tmc, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
			`)
		);
	}

	const damaArgs = [
		'NPMRDS Conflation 1.0',
		'gis_dataset',
		'osm_datasets.npmrds_conflation',
		[["OSM Conflation", "Road Network"]]
	]

	const options = {
		SQLITE_FILE_PATH,
		createConflationTableSql,
		queryConflationEdgesSql,
		makeCopyFromStream,
		incAmt: 20000,
		damaArgs
	}
	await loadConflation(options);
})()