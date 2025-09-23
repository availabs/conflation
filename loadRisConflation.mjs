import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';

import pgCopyStreams from "pg-copy-streams";

import loadConflation from "./loadConflation.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const SQLITE_FILE_PATH = join(__dirname, "TheConflationator", "RIS_checkpoints", "checkpoint-3.sqlite");

(async () => {
	const createConflationTableSql = `
		BEGIN;

		DROP TABLE IF EXISTS osm_datasets.ris_conflation;

		CREATE TABLE osm_datasets.ris_conflation(
			ogc_fid BIGSERIAL PRIMARY KEY,
			way_id BIGINT,
			ris_id TEXT,
			wkb_geometry GEOMETRY(LineString, 4032)
		);

		COMMIT;
	`;

	const queryConflationEdgesSql = `
		SELECT
				way_id,
				way_index,
				ris_id AS road_id,
				ris_index AS road_index,
				edge
			FROM ris_conflation;
	`;

	const makeCopyFromStream = client => {
		return client.query(
			pgCopyStreams.from(`
		  	COPY osm_datasets.ris_conflation(way_id, ris_id, wkb_geometry)
		  	FROM STDIN WITH (FORMAT TEXT, DELIMITER '|', NULL '')
			`)
		);
	}

	const options = {
		SQLITE_FILE_PATH,
		createConflationTableSql,
		queryConflationEdgesSql,
		makeCopyFromStream,
		incAmt: 50000,
		damaArgs: ['RIS Conflation 1.0', 'gis_dataset', 'osm_datasets.ris_conflation']
	}
	await loadConflation(options);
})()