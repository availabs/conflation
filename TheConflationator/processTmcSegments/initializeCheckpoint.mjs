import { initializeGraphAndQuadtree } from "../processSegments.mjs"

import { D3_INT_FORMAT } from "../constants.mjs"

async function reportStats(TheConflationator) {
  const numTMCsSql = `
    SELECT COUNT(DISTINCT tmc) as num_tmcs
      FROM tmcs;
  `
  const { num_tmcs } = TheConflationator.db.get(numTMCsSql);

  const numTMCsWithResultsSql = `
    SELECT COUNT(DISTINCT tmc) AS num_tmcs_with_results
      FROM tmc_results
  `
  const { num_tmcs_with_results } = TheConflationator.db.get(numTMCsWithResultsSql);

  TheConflationator.logInfo("FOUND PATHS FOR", D3_INT_FORMAT(num_tmcs_with_results), "TMCs OUT OF", D3_INT_FORMAT(num_tmcs), "TOTAL TMCs");
  TheConflationator.logInfo("MISSING PATHS FOR", D3_INT_FORMAT(num_tmcs - num_tmcs_with_results), "TMCs");

  const insertProblemTmcsSql = `
    INSERT INTO problem_tmcs(tmc, problem)
      SELECT DISTINCT tmc, 'no-path'
        FROM tmcs
          WHERE tmc NOT IN (
            SELECT DISTINCT tmc
              FROM tmc_results
          );
  `;
  TheConflationator.db.run(insertProblemTmcsSql);
  TheConflationator.logInfo("INSERTED PROBLEM TMCs INTO TABLE problem_tmcs");
}

export default async function initializeCheckpoint(TheConflationator) {

  const querySegmentsSql = `
    SELECT
        tmc AS road_id,
        ls_index,
        tmc_index AS road_index,
        miles,
        f_system,
        geojson
      FROM tmcs
        ORDER BY road_id,
                  ls_index ASC,
                  road_index ASC;
  `;

  const createResultsTableSql = `
    CREATE TABLE tmc_results(
      tmc TEXT,
      ls_index INT,
      tmc_index INT,
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
    INSERT INTO tmc_results(tmc, ls_index, tmc_index, path, rank, start_score, end_score, miles_score, miles)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
  `;
  const insertResultStmt = TheConflationator.db.prepare(insertResultSql);

  const querySegmentsWithNoResultsSql = `
    SELECT
        tmc AS road_id,
        ls_index,
        tmc_index AS road_index,
        miles,
        f_system,
        geojson
      FROM tmcs
        WHERE tmc NOT IN (
          SELECT DISTINCT tmc
            FROM tmc_results
        )
        ORDER BY road_id,
                  ls_index ASC,
                  road_index ASC;
  `;

  const updateSegmentSql = `
    INSERT OR REPLACE INTO tmcs(tmc, ls_index, tmc_index, miles, f_system, geojson)
      VALUES(?, ?, ?, ?, ?, ?)
  `;
  const updateSegmentStmt = TheConflationator.db.prepare(updateSegmentSql);

  const createProblemTmcsTableSql = `
    CREATE TABLE problem_tmcs(
      tmc TEXT,
      problem TEXT,
      PRIMARY KEY(tmc, problem)
    )
  `;
  TheConflationator.db.run(createProblemTmcsTableSql);

  return [
    ...initializeGraphAndQuadtree(TheConflationator),
    querySegmentsSql,
    insertResultStmt,
    querySegmentsWithNoResultsSql,
    updateSegmentStmt,
    reportStats
  ];
}