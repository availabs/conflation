import { quadtree as d3quadtree } from "d3-quadtree"
import { groups as d3groups } from "d3-array"

import * as turf from "@turf/turf";

import { MultiDirectedGraph } from "graphology"
import { dijkstra } from 'graphology-shortest-path';

import {
  D3_INT_FORMAT,
  NUM_RESULTS_TO_KEEP,
  HIGHWAY_TO_DIV_LENGTH_MAP,
  NO_OP
} from "./constants.mjs"

import {
  gatherEdges,
  processPath
} from "./gatherEdges.mjs"

export const initializeGraphAndQuadtree = TheConflationator => {

  const graph = new MultiDirectedGraph({ allowSelfLoops: false });

  let incAmt = 500000;
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

  const quadtree = d3quadtree().x(n => n.lon).y(n => n.lat);

  TheConflationator.logInfo("LOADING EDGES INTO GRAPH AND QUADTREE");
  const edgesIterator = TheConflationator.db.prepare("SELECT * FROM edges").iterate();
  for (const edge of edgesIterator) {
    const { from_node, to_node, highway } = edge;

    const edgeKey = `${ from_node }-${ to_node }`;

    graph.addEdgeWithKey(edgeKey, from_node, to_node, edge);

    const fromNode = graph.getNodeAttributes(from_node);
    const toNode = graph.getNodeAttributes(to_node);

    const qtEdge = {
      ...edge,
      edgeKey,
      edge: [[fromNode.lon, fromNode.lat], [toNode.lon, toNode.lat]]
    }

    quadtree.add({
      edge: qtEdge,
      lon: fromNode.lon,
      lat: fromNode.lat
    });

    const divLength = HIGHWAY_TO_DIV_LENGTH_MAP[highway];
    const divs = Math.ceil(edge.length / divLength);
    const oneDivth = edge.length / divs;

    for (let i = 1; i < divs - 1; ++i) {
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

  return [quadtree, graph, pathfinder];
}

async function processSegments(initialize, TheConflationator) {

  const [
    quadtree,
    graph,
    pathfinder,
    querySegmentsSql,
    insertResultStmt,
    querySegmentsWithNoResultsSql,
    updateSegmentStmt,
    reportStats = NO_OP,
    segmentIncAmt = 10000
  ] = await initialize(TheConflationator);

  const segments = TheConflationator.db.all(querySegmentsSql)
                      .map(segment => ({ ...segment, geometry: JSON.parse(segment.geojson) }));
  TheConflationator.logInfo("PROCESSING", D3_INT_FORMAT(segments.length), "SEGMENTS");

  // const segmentIncAmt = 10000;
  let logSegmentInfoAt = segmentIncAmt;
  let numSegments = 0;

  const logTimerInfoAt = 60000;
  let timestamp = Date.now();

  for (const segment of segments) {

    const { road_id, ls_index, road_index } = segment;

    processSegment(segment, quadtree, graph, pathfinder)
      .slice(0, NUM_RESULTS_TO_KEEP)
      .forEach(([path, calculatedMiles, sceRank, eceRank, mRank], i) => {
        insertResultStmt.run(road_id, ls_index, road_index,
                              JSON.stringify(path), i + 1,
                              sceRank, eceRank, mRank , calculatedMiles
                            );
      });

    if (++numSegments >= logSegmentInfoAt) {
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "SEGMENTS");
      logSegmentInfoAt += segmentIncAmt;
      timestamp = Date.now();
    }
    if ((Date.now() - timestamp) >= logTimerInfoAt) {
      TheConflationator.logInfo("I'M AAALIIIIIIVE!!!")
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "SEGMENTS");
      timestamp = Date.now();
    }
  }

  const segmentsWithNoResults = TheConflationator.db.all(querySegmentsWithNoResultsSql);

  const segmentGroups = d3groups(segmentsWithNoResults, s => s.road_id, s => s.ls_index);

  const reversedSegments = [];

  for (const [road_id, ls_segments] of segmentGroups) {
    for (const [ls_index, group] of ls_segments) {
      let i = 0;
      for (const segment of group) {
        // road_id,
        // ls_index,
        // road_index,
        // miles,
        // f_system,
        // geojson
        const geometry = {
          type: "LineString",
          coordinates: JSON.parse(segment.geojson).coordinates.reverse()
        }
        reversedSegments.push({
          ...segment,
          road_index: i++,
          geojson: JSON.stringify(geometry),
          geometry
        })
      }
    }
  }

  const reversedRoadIdsWithResults = new Set();

  TheConflationator.logInfo("PROCESSING", D3_INT_FORMAT(reversedSegments.length), "REVERSED SEGMENTS")
  for (const segment of reversedSegments) {

    const { road_id, ls_index, road_index } = segment;

    const results = processSegment(segment, quadtree, graph, pathfinder);

    if (results.length) {
      reversedRoadIdsWithResults.add(road_id);
    }

    results.slice(0, NUM_RESULTS_TO_KEEP)
      .forEach(([path, calculatedMiles, sceRank, eceRank, mRank], i) => {
        insertResultStmt.run(road_id, ls_index, road_index,
                              JSON.stringify(path), i + 1,
                              sceRank, eceRank, mRank, calculatedMiles
                            );
      });

    if (++numSegments >= logSegmentInfoAt) {
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "SEGMENTS");
      logSegmentInfoAt += segmentIncAmt;
      timestamp = Date.now();
    }
    if ((Date.now() - timestamp) >= logTimerInfoAt) {
      TheConflationator.logInfo("I'M AAALIIIIIIVE!!!")
      TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "SEGMENTS");
      timestamp = Date.now();
    }
  }

  for (const segment of reversedSegments) {
    if (reversedRoadIdsWithResults.has(segment.road_id)) {
      const {
        road_id, ls_index, road_index,
        miles, f_system, geojson
      } = segment;
      // road_id, ls_index, road_index, miles, f_system, geojson
      updateSegmentStmt.run(road_id, ls_index, road_index, miles, f_system, geojson);
    }
  }

  await reportStats(TheConflationator);
}
export default processSegments;

const processSegment = (segment, quadtree, graph, pathfinder) => {
  const {
    road_id, ls_index, road_index,
    miles, f_system, geometry
  } = segment;

  const linestring = [...geometry.coordinates];

  const seEdge = linestring.slice(0, 2);
  const startEdge = {
    road_id, f_system,
    edge: seEdge,
    bearing: turf.bearing(...seEdge),
    length: turf.distance(...seEdge, { units: "miles" })
  }

  const eeEdge = linestring.slice(-2);
  const endEdge = {
    road_id, f_system,
    edge: eeEdge,
    bearing: turf.bearing(...eeEdge),
    length: turf.distance(...eeEdge, { units: "miles" })
  }

  const topStartCandidateEdges = gatherEdges(startEdge, quadtree);
  const topEndCandidateEdges = gatherEdges(endEdge, quadtree);

  const results = [];

  for (const startCandidateEdge of topStartCandidateEdges) {
    for (const endCandidateEdge of topEndCandidateEdges) {
      results.push(
        processPath(graph, pathfinder(startCandidateEdge, endCandidateEdge),
                    miles, startCandidateEdge, endCandidateEdge
                  )
      );
    }
  }

  if (!topStartCandidateEdges.length) {
    for (const endCandidateEdge of topEndCandidateEdges) {
      results.push(
        processPath(graph, [endCandidateEdge.from_node, endCandidateEdge.to_node],
                    miles, endCandidateEdge, endCandidateEdge
                  )
      );
    }
  }
  else if (!topEndCandidateEdges.length) {
    for (const startCandidateEdge of topStartCandidateEdges) {
      results.push(
        processPath(graph, [startCandidateEdge.from_node, startCandidateEdge.to_node],
                    miles, startCandidateEdge, startCandidateEdge
                  )
      );
    }
  }
  return results.filter(([, calculatedMiles]) => calculatedMiles > 0.0)
                .sort((a, b) => (b[2] + b[3]) * b[4] - (a[2] + a[3]) * a[4]);
}