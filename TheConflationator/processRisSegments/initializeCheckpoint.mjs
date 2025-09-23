import { initializeGraphAndQuadtree } from "../processSegments.mjs"

import { D3_INT_FORMAT } from "../constants.mjs"

async function reportStats(TheConflationator) {
  const numRisSql = `
    SELECT COUNT(DISTINCT ris_id) as num_ris
      FROM ris;
  `
  const { num_ris } = TheConflationator.db.get(numRisSql);

  const numRisWithResultsSql = `
    SELECT COUNT(DISTINCT ris_id) AS num_ris_with_results
      FROM ris_results
  `
  const { num_ris_with_results } = TheConflationator.db.get(numRisWithResultsSql);

  TheConflationator.logInfo("FOUND PATHS FOR", D3_INT_FORMAT(num_ris_with_results), "RIS ROADWAYS OUT OF", D3_INT_FORMAT(num_ris), "TOTAL RIS ROADWAYS");
  TheConflationator.logInfo("MISSING PATHS FOR", D3_INT_FORMAT(num_ris - num_ris_with_results), "RIS ROADWAYS");

  const insertProblemRisSql = `
    INSERT INTO problem_ris(ris_id, problem)
      SELECT DISTINCT ris_id, 'no-path'
        FROM ris
          WHERE ris_id NOT IN (
            SELECT DISTINCT ris_id
              FROM ris_results
          );
  `;
  TheConflationator.db.run(insertProblemRisSql);
  TheConflationator.logInfo("INSERTED PROBLEM TMCs INTO TABLE problem_ris");
}

export default async function initializeCheckpoint(TheConflationator) {

  const querySegmentsSql = `
    SELECT
        ris_id AS road_id,
        ls_index,
        ris_index AS road_index,
        miles,
        f_system,
        geojson
      FROM ris
        ORDER BY road_id,
                  ls_index ASC,
                  road_index ASC;
  `;

  const createResultsTableSql = `
    CREATE TABLE ris_results(
      ris_id TEXT,
      ls_index INT,
      ris_index INT,
      path TEXT,
      rank INT,
      start_score DOUBLE PRECISION,
      end_score DOUBLE PRECISION,
      miles_score DOUBLE PRECISION,
      miles DOUBLE PRECISION
    )
  `;
  TheConflationator.db.run(createResultsTableSql);
  const insertResultSql = `
    INSERT INTO ris_results(ris_id, ls_index, ris_index, path, rank, start_score, end_score, miles_score, miles)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
  `;
  const insertResultStmt = TheConflationator.db.prepare(insertResultSql);

  const querySegmentsWithNoResultsSql = `
    SELECT
        ris_id AS road_id,
        ls_index,
        ris_index AS road_index,
        miles,
        f_system,
        geojson
      FROM ris
        WHERE ris_id NOT IN (
          SELECT DISTINCT ris_id
            FROM ris_results
        )
        ORDER BY road_id,
                  ls_index ASC,
                  road_index ASC
  `;

  const updateSegmentSql = `
    INSERT OR REPLACE INTO ris(ris_id, ls_index, ris_index, miles, f_system, geojson)
      VALUES(?, ?, ?, ?, ?, ?)
  `;
  const updateSegmentStmt = TheConflationator.db.prepare(updateSegmentSql);

  const createProblemRisTableSql = `
    CREATE TABLE problem_ris(
      ris_id TEXT,
      problem TEXT,
      PRIMARY KEY(ris_id, problem)
    )
  `;
  TheConflationator.db.run(createProblemRisTableSql);

  return [
    ...initializeGraphAndQuadtree(TheConflationator),
    querySegmentsSql,
    insertResultStmt,
    querySegmentsWithNoResultsSql,
    updateSegmentStmt,
    reportStats,
    50000
  ];
}