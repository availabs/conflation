import { pipeline, Readable } from "node:stream";
import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';
import {
  existsSync,
  mkdirSync,
  createWriteStream
} from "node:fs"

import {
  rollup as d3rollup,
  rollups as d3rollups
} from "d3-array"

import * as turf from "@turf/turf";

import { MultiDirectedGraph } from "graphology"
import { dijkstra } from 'graphology-shortest-path';

import {
  BEARING_SCALE,
  HIGHWAY_TO_F_SYSTEM_MAP,
  D3_INT_FORMAT
} from "../constants.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const TMC_LOGS_DIRECTORY = join(__dirname, "tmc_logs");

const DISTANCE_THRESHOLD = 5.0 / 5280.0;

const LOOK_BEHIND = -1;
const LOOK_AHEAD = 1;

async function runCheckpointThree(TheConflationator, graphFromCheckpoint2) {
  TheConflationator.logInfo("RUNNING CHECKPOINT THREE");

  const [
    getNewNodeId,
    insertFinalResultStmt,
    graph,
    pathfinder,
    insertProblemTmcStmt
  ] = await initializeCheckpointThree(TheConflationator, graphFromCheckpoint2);


/*
PROBLEM TMCs

104-07257 -- BAD LENGTH DUE TO SHORT ENDPOINTS

*/

  const queryResultsSql = `
    SELECT *
      FROM results
      WHERE rank = 1
      --AND tmc = '104-07257'
  `;
  const results = TheConflationator.db.all(queryResultsSql);

  const tmcIndexSorter = group => group.sort((a, b) => +a.tmc_index - +b.tmc_index);
  const resultRollups = d3rollups(results, tmcIndexSorter, r => r.tmc, r => r.linestring_index);

  TheConflationator.logInfo("PROCESSING", D3_INT_FORMAT(results.length), "RESULTS FROM", D3_INT_FORMAT(resultRollups.length), "TMCs");

  const queryTmcsSql = `
    SELECT *
      FROM tmcs
        WHERE tmc IN (
          SELECT DISTINCT tmc
            FROM results
              WHERE rank = 1
              --AND tmc = '104-07257'
        );
  `;
  const tmcs = TheConflationator.db.all(queryTmcsSql)
                      .map(tmc => ({ ...tmc,
                                      linestring: JSON.parse(tmc.geojson).coordinates
                                  })
                      );

  const tmcRollup = d3rollup(tmcs, tmcIndexSorter, r => r.tmc, r => r.linestring_index);

  const resultIncAmt = 10000;
  let logResultInfoAt = resultIncAmt;
  let numResults = 0;

  const tmcIncAmt = 2500;
  let logTmcInfoAt = tmcIncAmt;
  let numTMCs = 0;

  const logTimerInfoAt = 30000;
  let timestamp = Date.now();

  for (const [tmc, multilinestrings] of resultRollups) {

    const Geometry = {
      type: "MultiLineString",
      coordinates: []
    }

    for (const [lsIndex, lsSegments] of multilinestrings) {

      const osmNodes = [];

// THIS CODE COMBINES THE RANK 1 RESULTS INTO A SINGLE LINESTRING
      for (const segment of lsSegments) {

        const pathWithNodes = JSON.parse(segment.path);

        const isMinTmcIndex = !osmNodes.length;

        pathWithNodes.forEach(({ lon, lat, node_id }, i) => {
          if (isMinTmcIndex) {
            osmNodes.push(+node_id);
          }
          else {
            const index = osmNodes.indexOf(+node_id);
            if (index >= 0) {
              osmNodes.splice(index + 1, osmNodes.length - index);
            }
            else {
              const from_node = +osmNodes.at(-1);
              const to_node = +node_id;
              if (from_node !== to_node) {
                const edgeKey = `${ from_node }-${ to_node }`;
                if (!graph.hasEdge(edgeKey)) {
                  pathfinder(from_node, to_node).slice(1)
                    .forEach(node_id => {
                      osmNodes.push(+node_id);
                    })
                }
                else {
                  osmNodes.push(+node_id);
                }
              }
            }
          }
        })
      } // END for (const segment of lsSegments)

// CHECK FOR NOT-EXISTANT EDGES
      const [isOk, badEdges] = osmNodes.reduce((a, c, i, osmNodes) => {
        if (i === 0) return a;

        const from_node = osmNodes[i - 1];
        const to_node = osmNodes[i];

        const edgeKey = `${ from_node }-${ to_node }`;

        const okEdge = from_node !== to_node;
        const hasEdge = graph.hasEdge(edgeKey);

        const edgeIsOk = okEdge && hasEdge;

        const [isOk, bads] = a;

        return [
          isOk && edgeIsOk,
          !edgeIsOk ? [...bads, edgeKey] : bads
        ]
      }, [osmNodes.length > 1, []]);

      if (!isOk) {
        insertProblemTmcStmt.run(tmc, "bad-nodes");

        async function* yielder() {
          yield `NOT OK: ${ tmc }`;
          yield JSON.stringify(lsSegments.map(lss => ({ ...lss, path: JSON.parse(lss.path) })), null, 3);
          yield JSON.stringify(osmNodes, null, 3);
          yield JSON.stringify(badEdges, null, 3);
        }

        await new Promise((resolve, reject) => {
          pipeline(
            Readable.from(yielder()),
            createWriteStream(join(__dirname, "tmc_logs", `${ tmc }-${ lsIndex }.json`)),
            error => {
              if (error) {
                reject(error);
              }
              else {
                resolve();
              }
            }
          )
        });
      }
      else {

        const rankEdgeCandidate = (candidateEdge, currentEdge, direction) => {

          const candidateEdgeBearing = candidateEdge.bearing;
          const currentEdgeBearing = currentEdge.bearing;
          const bearingDiff = Math.abs(currentEdgeBearing - candidateEdgeBearing);
          const bearingValue = BEARING_SCALE(bearingDiff);

          const candidateEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[candidateEdge.highway] || 6.0;
          const currentEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[currentEdge.highway] || 6.0;
          const fSystemValue = 120.0 - Math.abs(candidateEdgeFsystem - currentEdgeFsystem) * 20.0;

          const wayIdBonus = (currentEdge.way_id === candidateEdge.way_id) &&
                              ((currentEdge.pos + direction) === candidateEdge.pos) ? 2 : 1

          return (bearingValue + fSystemValue) * wayIdBonus;
        }

        const tmcSegments = tmcRollup.get(tmc).get(lsIndex);
        const tmcLinestring = tmcSegments.reduce((a, c, i) => {
          if (i === 0) {
            return [...c.linestring];
          }
          return [
            ...a,
            ...c.linestring.slice(1)
          ]
        }, []);

        const tmcStartCoords = tmcLinestring.at(0);
        const tmcStartPoint = {
          type: "Point",
          coordinates: tmcStartCoords
        };

        const tmcEndCoords = tmcLinestring.at(-1);
        const tmcEndPoint = {
          type: "Point",
          coordinates: tmcEndCoords
        };

        const osmEdges = osmNodes.reduce((a, c, i, osmNodes) => {
          if (i === 0) return a;
          const from_node = osmNodes[i - 1];
          const to_node = osmNodes[i];
          const edgeKey = `${ from_node }-${ to_node }`;
          a.push(graph.getEdgeAttributes(edgeKey));
          return a;
        }, []);

        let tmcStartNodeCoords = null;
        let tmcStartNodeId = null;

        while (!tmcStartNodeCoords) {
          const currentOsmEdge = osmEdges.at(0);
          const { from_node, to_node } = currentOsmEdge;

          const osmFromNode = graph.getNodeAttributes(from_node);
          const osmFromCoords = [osmFromNode.lon, osmFromNode.lat];
          const osmFromPoint = {
            type: "Point",
            coordinates: osmFromCoords
          };

          const osmToNode = graph.getNodeAttributes(to_node);
          const osmToCoords = [osmToNode.lon, osmToNode.lat];
          const osmToPoint = {
            type: "Point",
            coordinates: osmToCoords
          };

          const osmEdge = [osmFromCoords, osmToCoords];
          const osmEdgeGeom = {
            type: "LineString",
            coordinates: osmEdge
          };

          const tmcPointOnOsmEdge = turf.nearestPointOnLine(osmEdgeGeom, tmcStartCoords, { units: "miles" }).geometry;
          const tmcCoordsOnOsmEdge = tmcPointOnOsmEdge.coordinates;

          const fromDist = turf.distance(osmFromCoords, tmcCoordsOnOsmEdge, { units: "miles" });
          const toDist = turf.distance(osmToCoords, tmcCoordsOnOsmEdge, { units: "miles" });
          
          if (turf.booleanEqual(osmFromPoint, tmcStartPoint)) {
            tmcStartNodeCoords = [...osmFromCoords];
            tmcStartNodeId = from_node;
          }
          else if (turf.booleanEqual(osmToPoint, tmcStartPoint)) {
            tmcStartNodeCoords = [...osmToCoords];
            tmcStartNodeId = to_node;
          }
          else if (turf.booleanEqual(osmFromPoint, tmcPointOnOsmEdge)) {
            const [topInboundEdge] = graph.inboundNeighbors(from_node)
                                      .map(node_id => {
                                        const edgeKey = `${ node_id }-${ from_node }`;
                                        return graph.getEdgeAttributes(edgeKey);
                                      })
                                      .map(edge => ({
                                        ...edge,
                                        rank: rankEdgeCandidate(edge, currentOsmEdge, LOOK_BEHIND)
                                      }))
                                      .sort((a, b) => b.rank - a.rank);

            if (!topInboundEdge) {
              tmcStartNodeCoords = [...osmFromCoords];
              tmcStartNodeId = from_node;
            }
            else {
              const osmFromNode = graph.getNodeAttributes(topInboundEdge.from_node);
              const osmFromCoords = [osmFromNode.lon, osmFromNode.lat];

              const osmToNode = graph.getNodeAttributes(topInboundEdge.to_node);
              const osmToCoords = [osmToNode.lon, osmToNode.lat];
              const osmToPoint = {
                type: "Point",
                coordinates: osmToCoords
              };

              const osmEdge = [osmFromCoords, osmToCoords];
              const osmEdgeGeom = {
                type: "LineString",
                coordinates: osmEdge
              };

              const tmcPointOnTopInboundEdge = turf.nearestPointOnLine(osmEdgeGeom, tmcStartCoords).geometry;

              if (turf.booleanEqual(osmToPoint, tmcPointOnTopInboundEdge)) {
                tmcStartNodeCoords = [...osmToCoords];
                tmcStartNodeId = topInboundEdge.to_node;
              }
              else {
                osmEdges.unshift(topInboundEdge);
              }
            }
          }
          else if (turf.booleanEqual(osmToPoint, tmcPointOnOsmEdge)) {
            if (osmEdges.length > 1) {
              osmEdges.shift();
            }
            else {
              tmcStartNodeCoords = [...osmToCoords];
              tmcStartNodeId = to_node;
            }
          }
          else if (fromDist <= DISTANCE_THRESHOLD) {
            tmcStartNodeCoords = [...osmFromCoords];
            tmcStartNodeId = from_node;
          }
          else if (toDist <= DISTANCE_THRESHOLD) {
            tmcStartNodeCoords = [...osmToCoords];
            tmcStartNodeId = to_node;
          }
          else {
            tmcStartNodeCoords = [...tmcCoordsOnOsmEdge];
            tmcStartNodeId = null;
          }
        } // END while

        let tmcEndNodeCoords = null;
        let tmcEndNodeId = null;

        while (!tmcEndNodeCoords) {
          const currentOsmEdge = osmEdges.at(-1);
          const { from_node, to_node } = currentOsmEdge;

          const osmFromNode = graph.getNodeAttributes(from_node);
          const osmFromCoords = [osmFromNode.lon, osmFromNode.lat];
          const osmFromPoint = {
            type: "Point",
            coordinates: osmFromCoords
          };

          const osmToNode = graph.getNodeAttributes(to_node);
          const osmToCoords = [osmToNode.lon, osmToNode.lat];
          const osmToPoint = {
            type: "Point",
            coordinates: osmToCoords
          };

          const osmEdgeGeom = {
            type: "LineString",
            coordinates: [osmFromCoords, osmToCoords]
          }

          const tmcPointOnOsmEdge = turf.nearestPointOnLine(osmEdgeGeom, tmcEndCoords).geometry;
          const tmcCoordsOnOsmEdge = tmcPointOnOsmEdge.coordinates;

          const fromDist = turf.distance(osmFromCoords, tmcCoordsOnOsmEdge, { units: "miles" });
          const toDist = turf.distance(osmToCoords, tmcCoordsOnOsmEdge, { units: "miles" });

          if (turf.booleanEqual(osmFromPoint, tmcEndPoint)) {
            tmcEndNodeCoords = [...osmFromCoords];
            tmcEndNodeId = from_node;
          }
          else if (turf.booleanEqual(osmToPoint, tmcEndPoint)) {
            tmcEndNodeCoords = [...osmToCoords];
            tmcEndNodeId = to_node;
          }
          else if (turf.booleanEqual(osmFromPoint, tmcPointOnOsmEdge)) {
            if (osmEdges.length > 1) {
              osmEdges.pop();
            }
            else {
              tmcEndNodeCoords = [...osmFromCoords];
              tmcEndNodeId = from_node;
            }
          }
          else if (turf.booleanEqual(osmToPoint, tmcPointOnOsmEdge)) {
            const [topOutboundEdge] = graph.outboundNeighbors(to_node)
                                        .map(node_id => {
                                          const edgeKey = `${ to_node }-${ node_id }`;
                                          return graph.getEdgeAttributes(edgeKey);
                                        })
                                        .map(edge => ({
                                          ...edge,
                                          rank: rankEdgeCandidate(edge, currentOsmEdge, LOOK_AHEAD)
                                        }))
                                        .sort((a, b) => b.rank - a.rank);

            if (!topOutboundEdge) {
              tmcEndNodeCoords = [...osmToCoords];
              tmcEndNodeId = to_node;
            }
            else {
              const osmFromNode = graph.getNodeAttributes(topOutboundEdge.from_node);
              const osmFromCoords = [osmFromNode.lon, osmFromNode.lat];
              const osmFromPoint = {
                type: "Point",
                coordinates: osmFromCoords
              };

              const osmToNode = graph.getNodeAttributes(topOutboundEdge.to_node);
              const osmToCoords = [osmToNode.lon, osmToNode.lat];

              const osmEdgeGeom = {
                type: "LineString",
                coordinates: [osmFromCoords, osmToCoords]
              };
              const tmcPointOnTopOutboundEdge = turf.nearestPointOnLine(osmEdgeGeom, tmcEndCoords).geometry;

              if (turf.booleanEqual(osmFromPoint, tmcPointOnTopOutboundEdge)) {
                tmcEndNodeCoords = [...osmFromCoords];
                tmcEndNodeId = topOutboundEdge.from_node;
              }
              else {
                osmEdges.push(topOutboundEdge);
              }
            }
          }
          else if (fromDist <= DISTANCE_THRESHOLD) {
            tmcEndNodeCoords = [...osmFromCoords];
            tmcEndNodeId = from_node;
          }
          else if (toDist <= DISTANCE_THRESHOLD) {
            tmcEndNodeCoords = [...osmToCoords];
            tmcEndNodeId = to_node;
          }
          else {
            tmcEndNodeCoords = [...tmcCoordsOnOsmEdge];
            tmcEndNodeId = null;
          }
        } // END while

        const oldEdges = [];
        const newEdges = [];

        const newOsmNodes = osmEdges.reduce((a, c, i, osmEdges) => {

          const { from_node, to_node } = c;

          const fromNode = graph.getNodeAttributes(from_node);
          const fromCoords = [fromNode.lon, fromNode.lat];

          const toNode = graph.getNodeAttributes(to_node);
          const toCoords = [toNode.lon, toNode.lat];

          if (i === 0) {
            if (tmcStartNodeId == from_node) {
              a.push(from_node, to_node);
            }
            else if (tmcStartNodeId == to_node) {
              a.push(to_node);
            }
            else {
              oldEdges.push(`${ from_node }-${ to_node }`);

              const newNodeId = getNewNodeId();
              const newNode = {
                node_id: newNodeId,
                lon: tmcStartNodeCoords[0],
                lat: tmcStartNodeCoords[1]
              };
              graph.addNode(newNodeId, newNode);

              const newEdge1 = {
                ...c,
                from_node,
                to_node: newNodeId,
                length: turf.distance(fromCoords, tmcStartNodeCoords, { units: "miles" }),
                bearing: turf.bearing(fromCoords, tmcStartNodeCoords)
              };
              const edgeKey1 = `${ from_node }-${ newNodeId }`;
              graph.addEdgeWithKey(edgeKey1, from_node, newNodeId, newEdge1);

              const newEdge2 = {
                ...c,
                from_node: newNodeId,
                to_node,
                pos: c.pos + 0.5,
                length: turf.distance(tmcStartNodeCoords, toCoords, { units: "miles" }),
                bearing: turf.bearing(tmcStartNodeCoords, toCoords)
              };
              const edgeKey2 = `${ newNodeId }-${ to_node }`;
              graph.addEdgeWithKey(edgeKey2, newNodeId, to_node, newEdge2);

              a.push(newNodeId, to_node);
            }
          }
          else if (i === (osmEdges.length - 1)) {
            if (tmcEndNodeId == from_node) {
              if (!a.includes(from_node)) {
                a.push(from_node);
              }
            }
            else if (tmcEndNodeId == to_node) {
              a.push(to_node);
            }
            else {
              oldEdges.push(`${ from_node }-${ to_node }`);

              const newNodeId = getNewNodeId();
              const newNode = {
                node_id: newNodeId,
                lon: tmcEndNodeCoords[0],
                lat: tmcEndNodeCoords[1]
              };
              graph.addNode(newNodeId, newNode);

              const newEdge1 = {
                ...c,
                from_node,
                to_node: newNodeId,
                length: turf.distance(fromCoords, tmcEndNodeCoords, { units: "miles" }),
                bearing: turf.bearing(fromCoords, tmcEndNodeCoords)
              };
              const edgeKey1 = `${ from_node }-${ newNodeId }`;
              graph.addEdgeWithKey(edgeKey1, from_node, newNodeId, newEdge1);

              const newEdge2 = {
                ...c,
                from_node: newNodeId,
                to_node,
                pos: c.pos + 0.5,
                length: turf.distance(tmcEndNodeCoords, toCoords, { units: "miles" }),
                bearing: turf.bearing(tmcEndNodeCoords, toCoords)
              };
              const edgeKey2 = `${ newNodeId }-${ to_node }`;
              graph.addEdgeWithKey(edgeKey2, newNodeId, to_node, newEdge2);

              a.push(newNodeId);
            }
          }
          else {
            a.push(to_node);
          }
          return a;
        }, []);

        const [isStillOk, badEdges] = newOsmNodes.reduce((a, c, i, newOsmNodes) => {
          if (i === 0) return a;

          const from_node = newOsmNodes[i - 1];
          const to_node = newOsmNodes[i];

          const edgeKey = `${ from_node }-${ to_node }`;

          const okEdge = from_node !== to_node;
          const hasEdge = graph.hasEdge(edgeKey);

          const edgeIsOk = okEdge && hasEdge;

          const [isOk, bads] = a;

          return [
            isOk && edgeIsOk,
            !edgeIsOk ? [...bads, edgeKey] : bads
          ];
        }, [true, []]);

        if (!isStillOk) {
          insertProblemTmcStmt.run(tmc, "bad-nodes");

          async function* yielder() {
            yield `NOT STILL OK: ${ tmc }`;
            yield JSON.stringify(lsSegments.map(lss => ({ ...lss, path: JSON.parse(lss.path) })), null, 3);
            yield JSON.stringify(osmNodes, null, 3);
            yield JSON.stringify(newOsmNodes, null, 3);
            yield JSON.stringify(badEdges, null, 3);
          }

          await new Promise((resolve, reject) => {
            pipeline(
              Readable.from(yielder()),
              createWriteStream(join(__dirname, "tmc_logs", `${ tmc }-${ lsIndex }.json`)),
              error => {
                if (error) {
                  reject(error);
                }
                else {
                  resolve();
                }
              }
            )
          });
        }
        else {

          const tmcMiles = tmcSegments.reduce((a, c) => {
            return a + c.miles;
          }, 0);

          const osmMiles = newOsmNodes.reduce((a, c, i, nodes) => {
            if (i === 0) return a;
            const edgeKey = `${ nodes[i - 1] }-${ nodes[i] }`;
            return a + graph.getEdgeAttribute(edgeKey, "length");
          }, 0);

          const relativeChange = Math.abs((osmMiles - tmcMiles) / tmcMiles);

          if (relativeChange > 0.05) {
            insertProblemTmcStmt.run(tmc, "bad-length");
          }

          const osmLinestring = newOsmNodes.map(node_id => {
            const { lon, lat } = graph.getNodeAttributes(node_id);
            return [+lon, +lat];
          });

          Geometry.coordinates.push(osmLinestring);
        }
      }
      if ((numResults += lsSegments.length) >= logResultInfoAt) {
        TheConflationator.logInfo("PROCESSED", D3_INT_FORMAT(numResults), "RESULTS");
        logResultInfoAt += resultIncAmt;
        timestamp = Date.now();
      }
      if ((Date.now() - timestamp) >= logTimerInfoAt) {
        TheConflationator.logInfo("I'M AAALIIIIIIVE!!!")
        TheConflationator.logInfo("PROCESSED", D3_INT_FORMAT(numResults), "RESULTS");
        TheConflationator.logInfo("PROCESSED", D3_INT_FORMAT(numTMCs), "TMCs");
        timestamp = Date.now();
      }
    } // END for (const [lsIndex, lsSegments] of multilinestrings)

    if (multilinestrings.length === Geometry.coordinates.length) {
      const miles = turf.length(Geometry, { units: "miles" });
      insertFinalResultStmt.run(tmc, miles, JSON.stringify(Geometry));
    }
    if (++numTMCs >= logTmcInfoAt) {
      TheConflationator.logInfo("PROCESSED", D3_INT_FORMAT(numTMCs), "TMCs");
      logTmcInfoAt += tmcIncAmt;
      timestamp = Date.now();
    }
  } // END for (const [tmc, multilinestrings] of resultRollups)

  await reportStatsForCheckpointThree(TheConflationator);
}
export default runCheckpointThree;

async function initializeCheckpointThree(TheConflationator, graphFromCheckpoint2) {

  if (!existsSync(TMC_LOGS_DIRECTORY)) {
    mkdirSync(TMC_LOGS_DIRECTORY);
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

  const createFinalResultsTableSql = `
    CREATE TABLE final_results(
      tmc TEXT PRIMARY KEY,
      miles DOUBLE PRECISION,
      wkb_geometry TEXT
    );
  `
  TheConflationator.db.run(createFinalResultsTableSql);

  const insertFinalResultSql = `
    INSERT INTO final_results(tmc, miles, wkb_geometry)
      VALUES(?, ?, ?);
  `;
  const insertFinalResultStmt = TheConflationator.db.prepare(insertFinalResultSql);

  const graph = graphFromCheckpoint2 || new MultiDirectedGraph({ allowSelfLoops: false });

  if (graphFromCheckpoint2) {
    TheConflationator.logInfo("USING GRAPH FROM CHECKPOINT 2");
  }
  else {
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
  }

  const pathfinder = (source, target) => {
    return dijkstra.bidirectional(graph, source, target, "length") || [];
  }

  const insertProblemTmcSql = `
    INSERT OR REPLACE INTO problem_tmcs(tmc, problem)
      VALUES(?, ?);
  `;
  const insertProblemTmcStmt = TheConflationator.db.prepare(insertProblemTmcSql);

  return [getNewNodeId, insertFinalResultStmt, graph, pathfinder, insertProblemTmcStmt];
}

async function reportStatsForCheckpointThree(TheConflationator) {
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