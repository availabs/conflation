export default function initializeCheckpoint(TheConflationator) {
	TheConflationator.db.run(`
  	CREATE TABLE IF NOT EXISTS ris(
			ris_id TEXT,
			ls_index INT,
			ris_index INT,
			miles DOUBLE PRECISION,
			f_system INT,
			geojson TEXT,
			PRIMARY KEY(ris_id, ls_index, ris_index)
  	);
	`);
	const insertRisSql = `
		INSERT INTO ris(ris_id, ls_index, ris_index, miles, f_system, geojson)
			VALUES(?, ?, ?, ?, ?, ?);
	`;
	const ris_insert_stmt = TheConflationator.db.prepare(insertRisSql);

	return [
    ris_insert_stmt
	]
}