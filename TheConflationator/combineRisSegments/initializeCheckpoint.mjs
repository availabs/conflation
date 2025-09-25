import { existsSync, mkdirSync } from "node:fs"
import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';

import { MultiDirectedGraph } from "graphology"
import { dijkstra } from 'graphology-shortest-path';

import * as turf from "@turf/turf";

import {
  D3_INT_FORMAT
} from "../constants.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const RIS_LOGS_DIRECTORY = join(__dirname, "ris_logs");

async function reportStats(TheConflationator) {
  const numNoPathsSql = `
    SELECT COUNT(1) AS num_no_paths
      FROM problem_ris
        WHERE problem = 'no-path'
  `;
  const { num_no_paths } = TheConflationator.db.get(numNoPathsSql);

  const numBadNodesSql = `
    SELECT COUNT(1) AS num_bad_nodes
      FROM problem_ris
        WHERE problem = 'bad-nodes'
  `;
  const { num_bad_nodes } = TheConflationator.db.get(numBadNodesSql);

  const numBadLengthsSql = `
    SELECT COUNT(1) AS num_bad_lengths
      FROM problem_ris
        WHERE problem = 'bad-length'
  `;
  const { num_bad_lengths } = TheConflationator.db.get(numBadLengthsSql);

  const numBadTMCsSql = `
    SELECT COUNT(DISTINCT ris_id) AS num_bad_tmcs
      FROM problem_ris
  `;
  const { num_bad_tmcs } = TheConflationator.db.get(numBadTMCsSql);

  TheConflationator.logInfo("NUMBER OF RIS ROADWAYS WITH NO PATHS:", D3_INT_FORMAT(num_no_paths));
  TheConflationator.logInfo("NUMBER OF RIS ROADWAYS WITH BAD NODES:", D3_INT_FORMAT(num_bad_nodes));
  TheConflationator.logInfo("NUMBER OF RIS ROADWAYS WITH BAD LENGTHS:", D3_INT_FORMAT(num_bad_lengths));
  TheConflationator.logInfo(D3_INT_FORMAT(num_bad_tmcs), "TOTAL PROBLEM RIS ROADWAYS");
}

export default async function initializeCheckpoint(TheConflationator) {

  if (!existsSync(RIS_LOGS_DIRECTORY)) {
    mkdirSync(RIS_LOGS_DIRECTORY);
  }

  const maxNodeIdSql = `
    SELECT MAX(node_id) AS max_node_id
      FROM nodes;
  `;
  const { max_node_id } = TheConflationator.db.get(maxNodeIdSql);
  let node_id = +max_node_id;
  const getNewNodeId = () => {
    return ++node_id;
  }
  
  const queryResultsSql = `
    SELECT
      ris_id AS road_id,
      ls_index,
      ris_index AS road_index,
      path,
      rank,
      start_score,
      end_score,
      miles_score,
      miles
    FROM ris_results
      WHERE rank = 1
  `;

  const querySegmentsSql = `
    SELECT
      ris_id AS road_id,
      ls_index,
      ris_index AS road_index,
      miles,
      f_system,
      geojson
    FROM ris
      WHERE ris_id IN (
        SELECT DISTINCT ris_id
          FROM ris_results
            WHERE rank = 1
      );
  `;

  const createFinalResultsTableSql = `
    CREATE TABLE final_ris_results(
      ris_id TEXT PRIMARY KEY,
      miles DOUBLE PRECISION,
      wkb_geometry TEXT
    );
  `;
  TheConflationator.db.run(createFinalResultsTableSql);

  const insertFinalResultSql = `
    INSERT INTO final_ris_results(ris_id, miles, wkb_geometry)
      VALUES(?, ?, ?);
  `;
  const insertFinalResultStmt = TheConflationator.db.prepare(insertFinalResultSql);

  const createRisConflationTableSql = `
    CREATE TABLE ris_conflation(
      ris_id TEXT,
      ris_index INT,
      from_node BIGINT,
      to_node BIGINT,
      PRIMARY KEY(ris_id, ris_index, from_node, to_node)
    );
  `;
  TheConflationator.db.run(createRisConflationTableSql);
  const createRisConflationTableIndexSql = `
    CREATE INDEX ris_from_to_idx
      ON ris_conflation(from_node, to_node);
  `;
  TheConflationator.db.run(createRisConflationTableIndexSql);
  const insertRisConflationSql = `
    INSERT OR REPLACE INTO ris_conflation(ris_id, ris_index, from_node, to_node)
      VALUES(?, ?, ?, ?);
  `;
  const insertRisConflationStmt = TheConflationator.db.prepare(insertRisConflationSql);

  const graph = new MultiDirectedGraph({ allowSelfLoops: false });

  const incAmt = 500000;
  let logInfoAt = incAmt;
  let numNodes = 0;

  TheConflationator.logInfo("LOADING NODES INTO GRAPH");
  const nodesIterator = TheConflationator.db.prepare("SELECT * FROM nodes").iterate();
  for (const node of nodesIterator) {
    graph.addNode(node.node_id, node);
    if (++numNodes >= logInfoAt) {
      TheConflationator.logInfo("LOADED", D3_INT_FORMAT(numNodes), "NODES");
      logInfoAt += incAmt;
    }
  }

  logInfoAt = incAmt;
  let numEdges = 0;

  TheConflationator.logInfo("LOADING EDGES INTO GRAPH");
  const edgesIterator = TheConflationator.db.prepare("SELECT * FROM edges").iterate();
  for (const edge of edgesIterator) {
    const { from_node, to_node } = edge;
    graph.addEdgeWithKey(`${ from_node }-${ to_node }`, from_node, to_node, edge);
    if (++numEdges >= logInfoAt) {
      TheConflationator.logInfo("LOADED", D3_INT_FORMAT(numEdges), "EDGES");
      logInfoAt += incAmt;
    }
  }

  const pathfinder = (source, target) => {
    return dijkstra.bidirectional(graph, source, target, "length") || [];
  }

  const insertProblemRisSql = `
    INSERT OR REPLACE INTO problem_ris(ris_id, problem)
      VALUES(?, ?);
  `;
  const insertProblemRisStmt = TheConflationator.db.prepare(insertProblemRisSql);

  const insertNodeSql = `
    INSERT OR REPLACE INTO nodes(node_id, lon, lat)
      VALUES($node_id, $lon, $lat);
  `;
  const insertNodeStmt = TheConflationator.db.prepare(insertNodeSql);

  const insertEdgeSql = `
    INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, bearing, length, highway, reversed)
      VALUES($way_id, $pos, $from_node, $to_node, $bearing, $length, $highway, $reversed);
  `
  const insertEdgeStmt = TheConflationator.db.prepare(insertEdgeSql);

  const dropEdgeSql = `
    DELETE FROM edges
      WHERE from_node = ?
        AND to_node = ?;
  `
  const dropEdgeStmt = TheConflationator.db.prepare(dropEdgeSql);

  let updateTmcNetwork = undefined;

// UPDATE NPMRDS, IF NEEDED
  if (TheConflationator.network === "NONE") {
    const selectEdgesToUpdateSql = `
      SELECT *
        FROM npmrds_conflation
          WHERE from_node = ?
            AND to_node = ?
    `;
    const selectEdgesToUpdateStmt = TheConflationator.db.prepare(selectEdgesToUpdateSql);

    const insertNewEdgeSql = `
      INSERT INTO npmrds_conflation(tmc, tmc_index, from_node, to_node)
        VALUES(?, ?, ?, ?);
    `;
    const insertNewEdgeStmt = TheConflationator.db.prepare(insertNewEdgeSql);

    const deleteOldEdgeSql = `
      DELETE FROM npmrds_conflation
        WHERE from_node = ?
          AND to_node = ?
    `;
    const deleteOldEdgeStmt = TheConflationator.db.prepare(deleteOldEdgeSql);

    updateTmcNetwork = update => {
      const [from_node, new_node, to_node] = update;

      const edgesToUpdate = selectEdgesToUpdateStmt.all(from_node, to_node);

      for (const { tmc, tmc_index } of edgesToUpdate) {
        insertNewEdgeStmt.run(tmc, tmc_index, from_node, new_node);
        insertNewEdgeStmt.run(tmc, tmc_index + 0.1, new_node, to_node);
        deleteOldEdgeStmt.run(from_node, to_node);
      }
    }
  }

  return [
    RIS_LOGS_DIRECTORY,
    getNewNodeId,
    queryResultsSql,
    querySegmentsSql,
    insertFinalResultStmt,
    insertRisConflationStmt,
    insertProblemRisStmt,
    insertNodeStmt,
    insertEdgeStmt,
    dropEdgeStmt,
    graph, pathfinder,
    reportStats,
    updateTmcNetwork
  ];
}