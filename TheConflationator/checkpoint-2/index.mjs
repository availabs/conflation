import { scaleLinear as d3scaleLinear } from "d3-scale"
import { quadtree as d3quadtree } from "d3-quadtree"

import * as turf from "@turf/turf";

import { MultiDirectedGraph } from "graphology"
import { dijkstra } from 'graphology-shortest-path';

import {
  BEARING_SCALE,
  HIGHWAY_TO_F_SYSTEM_MAP,
  D3_INT_FORMAT
} from "../constants.mjs"

const MAX_F_SYSTEM_RANK = 120.0;

const MAX_FEET = 50.0;
const MIN_FEET = 25.0;
const FEET_TO_MILES = 1.0 / 5280.0;
const BUFFER_SCALE = d3scaleLinear([1, 7], [MAX_FEET * FEET_TO_MILES, MIN_FEET * FEET_TO_MILES]);

const MAX_MILES_RANK = 60.0;
const MIN_MILES_TO_RANK = 0.1;

const NUM_EDGES_TO_KEEP = 3;
const NUM_RESULTS_TO_KEEP = 3;

async function runCheckpointTwo(TheConflationator) {
  TheConflationator.logInfo("RUNNING CHECKPOINT TWO");

  const [
    quadtree,
    graph,
    pathfinder,
    insertResultStmt
  ] = await initializeCheckpointTwo(TheConflationator);

/*
PROBLEM TMCs

HIGHER VALUE RANKING FOR F_SYSTEM MIGHT HELP TRUNKS AND PRIMARIES

104-06912 -- BAD EDGE SELECTION, SMALLER BUFFER MIGHT FIX
120N05355 -- BAD EDGE SELECTION, SMALLER BUFFER MIGHT FIX
120-05354 -- BAD EDGE SELECTION, SMALLER BUFFER MIGHT FIX
120+06575 -- BAD EDGE SELECTION, SMALLER BUFFER MIGHT FIX
120+09555 -- BAD EDGE SELECTION, SMALLER BUFFER MIGHT FIX
104+07878 -- BAD EDGE SELECTION, SMALLER BUFFER MIGHT FIX

120P24691 -- BAD EDGE SELECTION, SELECTED OSM EDGES ARE BEYOND TMC EDGES

120N08309 -- BAD EDGE SELECTION DUE TO BAD TMC SHAPE
120N08024 -- BAD EDGE SELECTION DUE TO BAD TMC SHAPE

120+11604 -- BAD EDGE SELECTION

120-23211 -- DOESN'T SEEM TO BE A ROAD, MIGHT BE MISSING A BRIDGE

*/

  const rankEdgeCandidate = (candidateEdge, tmcEdge) => {
    const candidateEdgeBearing = candidateEdge.bearing;
    const tmcEdgeBearing = tmcEdge.bearing;
    const bearingDiff = Math.abs(tmcEdgeBearing - candidateEdgeBearing);
    const bearingValue = BEARING_SCALE(bearingDiff);

    const candidateEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[candidateEdge.highway];
    const fSystemValue = MAX_F_SYSTEM_RANK - Math.abs(+tmcEdge.f_system - candidateEdgeFsystem) * (MAX_F_SYSTEM_RANK / 6);

    return bearingValue + fSystemValue;
  }
// END rankEdgeCandidate

  const findAllWithin = tmcEdge => {
    const tmcEdgeGeometry = {
      type: "LineString",
      coordinates: [...tmcEdge.edge]
    }
    const buffered = turf.buffer(tmcEdgeGeometry, BUFFER_SCALE(+tmcEdge.f_system), { units: "miles" });

    const [xmin, ymin, xmax, ymax] = turf.bbox(buffered);

    const edgeSet = new Set();

    quadtree.visit((node, x1, y1, x2, y2) => {
      if (!node.length) {
        do {
          const d = node.data;

          if (edgeSet.has(d.edge)) continue;

          const p = {
            type: "Point",
            coordinates: [d.lon, d.lat]
          }
          const ls = {
            type: "LineString",
            coordinates: [...d.edge.edge]
          }
          if (turf.booleanContains(buffered, p)) {
            edgeSet.add(d.edge);
          }
          else if (turf.booleanIntersects(buffered, ls)) {
            edgeSet.add(d.edge);
          }
        } while (node = node.next);
      }
      return x1 >= xmax || y1 >= ymax || x2 < xmin || y2 < ymin;
    });

    return [...edgeSet];
  }
// END findAllWithin

  const queryTmcsSql = `
    SELECT *
      FROM tmcs
        WHERE tmc NOT LIKE 'C%'
          ORDER BY tmc,
                    linestring_index ASC,
                    tmc_index ASC;
  `;
  const segments = TheConflationator.db.all(queryTmcsSql)
                      .map(segment => ({ ...segment, geometry: JSON.parse(segment.geojson) }));

  const tmcSet = segments.reduce((a, c) => {
    return a.add(c.tmc);
  }, new Set());
  TheConflationator.logInfo("PROCESSING", D3_INT_FORMAT(segments.length), "TMC SEGMENTS DERIVED FROM", D3_INT_FORMAT(tmcSet.size), "TMCs");

  const segmentIncAmt = 10000;
  let logSegmentInfoAt = segmentIncAmt;
  let numSegments = 0;

  const tmcIncAmt = 5000;
  let logTmcInfoAt = tmcIncAmt;
  const processedTMCs = new Set();

  const logTimerInfoAt = 30000;
  let timestamp = Date.now();

  for (const segment of segments) {
    const {
      tmc, linestring_index, tmc_index,
      miles, f_system, geometry
    } = segment;

    processedTMCs.add(tmc);

    const linestring = [...geometry.coordinates];

    const bearingLimit = 45.0;

    const seEdge = linestring.slice(0, 2);
    const startEdge = {
      tmc, f_system,
      edge: seEdge,
      bearing: turf.bearing(...seEdge),
      length: turf.distance(...seEdge, { units: "miles" })
    }
    const seBearingDiff = sec => Math.abs(startEdge.bearing - sec.bearing) < bearingLimit;

    const eeEdge = linestring.slice(-2);
    const endEdge = {
      tmc, f_system,
      edge: eeEdge,
      bearing: turf.bearing(...eeEdge),
      length: turf.distance(...eeEdge, { units: "miles" })
    }
    const eeBearingDiff = eec => Math.abs(endEdge.bearing - eec.bearing) < bearingLimit;

    const startCandidateEdges = findAllWithin(startEdge).filter(seBearingDiff);
    for (const candidateEdge of startCandidateEdges) {
      candidateEdge.rank = rankEdgeCandidate(candidateEdge, startEdge);
    }
    const topStartCandidateEdges = startCandidateEdges.sort((a, b) => b.rank - a.rank)
                                                      .slice(0, NUM_EDGES_TO_KEEP);

    const endCandidateEdges = findAllWithin(endEdge).filter(eeBearingDiff);
    for (const candidateEdge of endCandidateEdges) {
      candidateEdge.rank = rankEdgeCandidate(candidateEdge, endEdge);
    }
    const topEndCandidateEdges = endCandidateEdges.sort((a, b) => b.rank - a.rank)
                                                  .slice(0, NUM_EDGES_TO_KEEP);

    const results = [];

    for (const startCandidateEdge of topStartCandidateEdges) {
      for (const endCandidateEdge of topEndCandidateEdges) {

        const path = pathfinder(startCandidateEdge, endCandidateEdge);

        const pathWithNodes = path.map(nid => graph.getNodeAttributes(nid));

        const calculatedMiles = path.reduce((a, c, i, path) => {
          if (i === 0) return a;
          const edgeKey = `${ path[i - 1] }-${ path[i] }`;
          return a + graph.getEdgeAttribute(edgeKey, "length");
        }, 0);

        let milesRank = MAX_MILES_RANK;

        if (miles >= MIN_MILES_TO_RANK) {
          milesRank *= (Math.min(calculatedMiles, miles) / Math.max(calculatedMiles, miles));
        }

        results.push([pathWithNodes, calculatedMiles, startCandidateEdge.rank, endCandidateEdge.rank, milesRank]);
      }
    }

    if (!topStartCandidateEdges.length) {
      for (const endCandidateEdge of topEndCandidateEdges) {

        const path = [endCandidateEdge.from_node, endCandidateEdge.to_node];

        const pathWithNodes = path.map(nid => graph.getNodeAttributes(nid));

        const calculatedMiles = path.reduce((a, c, i, path) => {
          if (i === 0) return a;
          const edgeKey = `${ path[i - 1] }-${ path[i] }`;
          return a + graph.getEdgeAttribute(edgeKey, "length");
        }, 0);

        let milesRank = MAX_MILES_RANK;

        if (miles >= MIN_MILES_TO_RANK) {
          milesRank *= (Math.min(calculatedMiles, miles) / Math.max(calculatedMiles, miles));
        }

        results.push([pathWithNodes, calculatedMiles, endCandidateEdge.rank, endCandidateEdge.rank, milesRank]);
      }
    }
    else if (!topEndCandidateEdges.length) {
      for (const startCandidateEdge of topStartCandidateEdges) {

        const path = [startCandidateEdge.from_node, startCandidateEdge.to_node];

        const pathWithNodes = path.map(nid => graph.getNodeAttributes(nid));

        const calculatedMiles = path.reduce((a, c, i, path) => {
          if (i === 0) return a;
          const edgeKey = `${ path[i - 1] }-${ path[i] }`;
          return a + graph.getEdgeAttribute(edgeKey, "length");
        }, 0);

        let milesRank = MAX_MILES_RANK;

        if (miles >= MIN_MILES_TO_RANK) {
          milesRank *= (Math.min(calculatedMiles, miles) / Math.max(calculatedMiles, miles));
        }

        results.push([pathWithNodes, calculatedMiles, startCandidateEdge.rank, startCandidateEdge.rank, milesRank]);
      }
    }

    results
      .filter(([, calculatedMiles]) => calculatedMiles > 0.0)
      .sort((a, b) => (b[2] + b[3] + b[4]) - (a[2] + a[3] + a[4]))
      .slice(0, NUM_RESULTS_TO_KEEP)
      .forEach(([path, calculatedMiles, sceRank, eceRank, mRank], i) => {
        insertResultStmt.run(tmc, linestring_index, tmc_index, JSON.stringify(path), i + 1, sceRank, eceRank, mRank , calculatedMiles);
      });

    if (++numSegments >= logSegmentInfoAt) {
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "TMC SEGMENTS");
      logSegmentInfoAt += segmentIncAmt;
      timestamp = Date.now();
    }
    if (processedTMCs.size >= logTmcInfoAt) {
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(processedTMCs.size), "TMCs");
      logTmcInfoAt += tmcIncAmt;
      timestamp = Date.now();
    }
    if ((Date.now() - timestamp) >= logTimerInfoAt) {
      TheConflationator.logInfo("I'M AAALIIIIIIVE!!!")
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "TMC SEGMENTS");
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(processedTMCs.size), "TMCs");
      timestamp = Date.now();
    }
  }

  await reportStatsForCheckpointTwo(TheConflationator);

  return graph;
}
export default runCheckpointTwo;

async function initializeCheckpointTwo(TheConflationator) {

  const quadtree = d3quadtree().x(n => n.lon).y(n => n.lat);

  const graph = new MultiDirectedGraph({ allowSelfLoops: false });

  const incAmt = 500000;
  let logInfoAt = incAmt;
  let numNodes = 0;

  TheConflationator.logInfo("LOADING NODES INTO GRAPH");
  const nodesSql = `
    SELECT node_id, lon, lat
      FROM nodes
  `;
  const nodesIterator = TheConflationator.db.prepare(nodesSql).iterate();
  for (const node of nodesIterator) {
    graph.addNode(node.node_id, node);
    if (++numNodes >= logInfoAt) {
      TheConflationator.logInfo("LOADED", D3_INT_FORMAT(numNodes), "NODES");
      logInfoAt += incAmt;
    }
  }

  logInfoAt = incAmt;
  let numEdges = 0;

  TheConflationator.logInfo("LOADING EDGES INTO GRAPH AND QUADTREE");
  const edgesIterator = TheConflationator.db.prepare("SELECT * FROM edges").iterate();
  for (const edge of edgesIterator) {
    const { from_node, to_node } = edge;

    graph.addEdgeWithKey(`${ from_node }-${ to_node }`, from_node, to_node, edge);

    const fromNode = graph.getNodeAttributes(from_node);
    const toNode = graph.getNodeAttributes(to_node);

    const qtEdge = {
      ...edge,
      edge: [[fromNode.lon, fromNode.lat], [toNode.lon, toNode.lat]]
    }

    quadtree.add({
      edge: qtEdge,
      lon: fromNode.lon,
      lat: fromNode.lat
    });

    const divLength = 100.0 / 5280.0;
    let div = Math.max(2, Math.ceil(edge.length / divLength));
    if ((div % 2) === 0) ++div;

    const oneDivth = edge.length / div;

    for (let i = 1; i < div; ++i) {
      const [lon, lat] = turf.destination([fromNode.lon, fromNode.lat],
                                          oneDivth * i,
                                          edge.bearing,
                                          { units: "miles" }).geometry.coordinates;

      quadtree.add({
        edge: qtEdge,
        lon,
        lat
      });
    }

    quadtree.add({
      edge: qtEdge,
      lon: toNode.lon,
      lat: toNode.lat
    });

    if (++numEdges >= logInfoAt) {
      TheConflationator.logInfo("LOADED", D3_INT_FORMAT(numEdges), "EDGES");
      logInfoAt += incAmt;
    }
  }

  const pathfinder = (sourceEdge, targetEdge) => {
    return dijkstra.bidirectional(graph, sourceEdge.from_node, targetEdge.to_node, "length") || [];
  }

  const createResultsTableSql = `
    CREATE TABLE results(
      tmc TEXT,
      linestring_index INT,
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
    INSERT INTO results(tmc, linestring_index, tmc_index, path, rank, start_score, end_score, miles_score, miles)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
  `;
  const insertResultStmt = TheConflationator.db.prepare(insertResultSql);

  return [
    quadtree,
    graph,
    pathfinder,
    insertResultStmt
  ];
}

async function reportStatsForCheckpointTwo(TheConflationator) {
  const numTMCsSql = `
    SELECT COUNT(DISTINCT tmc) as num_tmcs
      FROM tmcs
        WHERE tmc NOT LIKE 'C%';
  `
  const { num_tmcs } = TheConflationator.db.get(numTMCsSql);

  const numTMCsWithResultsSql = `
    SELECT COUNT(DISTINCT tmc) AS num_tmcs_with_results
      FROM results
  `
  const { num_tmcs_with_results } = TheConflationator.db.get(numTMCsWithResultsSql);

  TheConflationator.logInfo("FOUND PATHS FOR", D3_INT_FORMAT(num_tmcs_with_results), "TMCs OUT OF", D3_INT_FORMAT(num_tmcs), "TOTAL TMCs");
  TheConflationator.logInfo("MISSING PATHS FOR", D3_INT_FORMAT(num_tmcs - num_tmcs_with_results), "TMCs");

  const createProblemTmcsTableSql = `
    CREATE TABLE problem_tmcs(
      tmc TEXT,
      problem TEXT,
      PRIMARY KEY(tmc, problem)
    )
  `;
  TheConflationator.db.run(createProblemTmcsTableSql);
  TheConflationator.logInfo("CREATED problem_tmcs TABLE");
  const insertProblemTmcsSql = `
    INSERT INTO problem_tmcs(tmc, problem)
      SELECT DISTINCT tmc, 'no-path'
        FROM tmcs
          WHERE tmc NOT IN (
            SELECT DISTINCT tmc
              FROM results
          )
          AND tmc NOT LIKE 'C%';
  `;
  TheConflationator.db.run(insertProblemTmcsSql);
  TheConflationator.logInfo("INSERTED PROBLEM TMCs INTO TABLE problem_tmcs");
}