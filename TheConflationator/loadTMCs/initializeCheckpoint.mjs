export default function initializeCheckpoint(TheConflationator) {
	TheConflationator.db.run(`
  	CREATE TABLE IF NOT EXISTS tmcs(
			tmc TEXT,
			ls_index INT,
			tmc_index INT,
			miles DOUBLE PRECISION,
			f_system INT,
			geojson TEXT,
			PRIMARY KEY(tmc, ls_index, tmc_index)
  	);
	`);
	const insertTmcSql = `
		INSERT INTO tmcs(tmc, ls_index, tmc_index, miles, f_system, geojson)
			VALUES(?, ?, ?, ?, ?, ?);
	`;
	const segment_insert_stmt = TheConflationator.db.prepare(insertTmcSql);

	return [
    segment_insert_stmt
	]
}