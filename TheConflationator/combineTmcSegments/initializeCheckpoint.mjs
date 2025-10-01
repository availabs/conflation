// import { existsSync, mkdirSync } from "node:fs"
// import { join, dirname } from "node:path";
// import { fileURLToPath } from 'node:url';

import { MultiDirectedGraph } from "graphology"
import { dijkstra } from 'graphology-shortest-path';

import {
  D3_INT_FORMAT
} from "../constants.mjs"

// const __filename = fileURLToPath(import.meta.url);
// const __dirname = dirname(__filename);

// const TMC_LOGS_DIRECTORY = join(__dirname, "tmc_logs");

async function reportStats(TheConflationator) {
  const numNoPathsSql = `
    SELECT COUNT(1) AS num_no_paths
      FROM problem_tmcs
        WHERE problem = 'no-path'
  `;
  const { num_no_paths } = TheConflationator.db.get(numNoPathsSql);

  const numBadNodesSql = `
    SELECT COUNT(1) AS num_bad_nodes
      FROM problem_tmcs
        WHERE problem = 'bad-nodes'
  `;
  const { num_bad_nodes } = TheConflationator.db.get(numBadNodesSql);

  const numBadLengthsSql = `
    SELECT COUNT(1) AS num_bad_lengths
      FROM problem_tmcs
        WHERE problem = 'bad-length'
  `;
  const { num_bad_lengths } = TheConflationator.db.get(numBadLengthsSql);

  const numBadTMCsSql = `
    SELECT COUNT(DISTINCT tmc) AS num_bad_tmcs
      FROM problem_tmcs
  `;
  const { num_bad_tmcs } = TheConflationator.db.get(numBadTMCsSql);

  TheConflationator.logInfo("NUMBER OF TMCs WITH NO PATHS:", D3_INT_FORMAT(num_no_paths));
  TheConflationator.logInfo("NUMBER OF TMCs WITH BAD NODES:", D3_INT_FORMAT(num_bad_nodes));
  TheConflationator.logInfo("NUMBER OF TMCs WITH BAD LENGTHS:", D3_INT_FORMAT(num_bad_lengths));
  TheConflationator.logInfo(D3_INT_FORMAT(num_bad_tmcs), "TOTAL PROBLEM TMCs");
}

export default async function initializeCheckpoint(TheConflationator) {

  // if (!existsSync(TMC_LOGS_DIRECTORY)) {
  //   mkdirSync(TMC_LOGS_DIRECTORY);
  // }

  const maxNodeIdSql = `
    SELECT MAX(node_id) AS max_node_id
      FROM nodes;
  `;
  const { max_node_id } = TheConflationator.db.get(maxNodeIdSql);
  let node_id = +max_node_id;
  const getNewNodeId = () => {
    return ++node_id;
  }
  
  // tmc, ls_index, tmc_index, path, rank, start_score, end_score, miles_score, miles
  const queryResultsSql = `
    SELECT
        tmc AS road_id,
        ls_index,
        tmc_index AS road_index,
        path,
        rank,
        start_score,
        end_score,
        miles_score,
    miles
      FROM tmc_results
        WHERE rank = 1
          --AND tmc = '104+10583'
  `;

  const querySegmentsSql = `
    SELECT
      tmc AS road_id,
      ls_index,
      tmc_index AS road_index,
      miles,
      f_system,
      geojson
    FROM tmcs
      WHERE tmc IN (
        SELECT DISTINCT tmc
          FROM tmc_results
            WHERE rank = 1
      );
  `;

  const createFinalResultsTableSql = `
    CREATE TABLE final_tmc_results(
      tmc TEXT PRIMARY KEY,
      miles DOUBLE PRECISION,
      wkb_geometry TEXT
    );
  `;
  TheConflationator.db.run(createFinalResultsTableSql);

  const insertFinalResultSql = `
    INSERT INTO final_tmc_results(tmc, miles, wkb_geometry)
      VALUES(?, ?, ?);
  `;
  const insertFinalResultStmt = TheConflationator.db.prepare(insertFinalResultSql);

  const createNpmrdsConflationTableSql = `
    CREATE TABLE npmrds_conflation(
      tmc TEXT,
      tmc_index INT,
      from_node BIGINT,
      to_node BIGINT,
      PRIMARY KEY(tmc, tmc_index, from_node, to_node)
    );
  `;
  TheConflationator.db.run(createNpmrdsConflationTableSql);
  const createNpmrdsConflationTableIndexSql = `
    CREATE INDEX npmrds_from_to_idx
      ON npmrds_conflation(from_node, to_node);
  `;
  TheConflationator.db.run(createNpmrdsConflationTableIndexSql);
  const insertNpmrdsConflationSql = `
    INSERT OR IGNORE INTO npmrds_conflation(tmc, tmc_index, from_node, to_node)
      VALUES(?, ?, ?, ?);
  `;
  const insertNpmrdsConflationStmt = TheConflationator.db.prepare(insertNpmrdsConflationSql);

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

  const insertProblemTmcSql = `
    INSERT OR REPLACE INTO problem_tmcs(tmc, problem)
      VALUES(?, ?);
  `;
  const insertProblemTmcStmt = TheConflationator.db.prepare(insertProblemTmcSql);

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

  return [
    getNewNodeId,
    queryResultsSql,
    querySegmentsSql,
    insertFinalResultStmt,
    insertNpmrdsConflationStmt,
    insertProblemTmcStmt,
    insertNodeStmt,
    insertEdgeStmt,
    dropEdgeStmt,
    graph, pathfinder,
    reportStats
  ];
}