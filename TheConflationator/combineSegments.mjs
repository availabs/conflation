import { pipeline, Readable } from "node:stream";
import { join, dirname } from "node:path";
import { fileURLToPath } from 'node:url';
import { createWriteStream } from "node:fs"

import {
  rollup as d3rollup,
  group as d3group,
  rollups as d3rollups
} from "d3-array"

import * as turf from "@turf/turf";

import {
  BEARING_SCALE,
  MAX_F_SYSTEM_RANK,
  HIGHWAY_TO_F_SYSTEM_MAP,
  D3_INT_FORMAT,
  DISTANCE_THRESHOLD,
  LOOK_AHEAD,
  LOOK_BEHIND,
  DISTANCE_RELATIVE_CHANGE,
  DISTANCE_DIFFERENCE,
  NO_OP
} from "./constants.mjs"

// const distSquared = (p1, p2) => {
//   return ((p2[0] - p1[0]) * (p2[0] - p1[0])) + ((p2[1] - p1[1]) * (p2[1] - p1[1]));
// }

const findClosestPoint = (osmFromNode, osmToNode, roadLinestring, direction) => {

  const linestring = direction === LOOK_AHEAD ?
                                    [...roadLinestring].reverse() :
                                    roadLinestring;

  const e1 = [osmFromNode.lon, osmFromNode.lat];
  const e2 = [osmToNode.lon, osmToNode.lat];
  const p = direction === LOOK_AHEAD ? e2 : e1;

  const [closest, index, dist1] = linestring.reduce((a, c, i) => {
    const dist = turf.distance(p, c);
    if (dist <= a[2]) {
      return [c, i, dist];
    }
    return a;
  }, [null, -1, Infinity]);

  return linestring[Math.max(index - 1), 0];
}

const filterUnwantedEdges = (lsSegments, graph, pathfinder) => {

  const highwaysMap = {};
  // const wayIdsMap = {};

  const allEdgeKeys = [];

  let totalLength = 0;

  for (const segment of lsSegments) {
    const path = JSON.parse(segment.path);

    for (let i = 1; i < path.length; ++i) {
      const from_node = path[i - 1].node_id;
      const to_node = path[i].node_id;
      const edgeKey = `${ from_node }-${ to_node }`;

      allEdgeKeys.push(edgeKey);

      const { highway, way_id, length } = graph.getEdgeAttributes(edgeKey);

      totalLength += length;

      if (!(highway in highwaysMap)) {
        highwaysMap[highway] = {
          highway,
          edgeKeys: [],
          length: 0
        };
      }
      highwaysMap[highway].length += length;
      highwaysMap[highway].edgeKeys.push(edgeKey);

      // if (!(way_id in wayIdsMap)) {
      //   wayIdsMap[way_id] = {
      //     way_id,
      //     edgeKeys: [],
      //     length: 0
      //   };
      // }
      // wayIdsMap[way_id].length += length;
      // wayIdsMap[way_id].edgeKeys.push(edgeKey);
    }
  }

  const badEdgeKeys = new Set();

  const highways = Object.values(highwaysMap).sort((a, b) => a.length - b.length);
  let cutoff = totalLength * 0.05;
  highways.forEach(({ edgeKeys, length }) => {
    if (length <= cutoff) {
      edgeKeys.forEach(ek => badEdgeKeys.add(ek));
    }
  });

  // if (totalLength >= 0.25) {
  //   const wayIds = Object.values(wayIdsMap).sort((a, b) => a.length - b.length);
  //   cutoff = totalLength * 0.005;
  //   wayIds.forEach(({ edgeKeys, length }) => {
  //     if (length <= cutoff) {
  //       edgeKeys.forEach(ek => badEdgeKeys.add(ek));
  //     }
  //   });
  // }

  const osmNodes = [];

  allEdgeKeys.filter(ek => !badEdgeKeys.has(ek))
    .reduce((a, c) => {
      const [from_node, to_node] = c.split("-").map(Number);
      if (a.at(-1) !== from_node) {
        a.push(from_node, to_node);
      }
      else if (a.at(-1) !== to_node) {
        a.push(to_node);
      }
      return a;
    }, [])
    .forEach((node_id, i) => {
      if (i === 0) {
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
    });

  return osmNodes;
}

const checkNodes = (nodes, graph) => {
  return nodes.reduce((a, c, i, nodes) => {
    if (i === 0) return a;

    const from_node = nodes[i - 1];
    const to_node = nodes[i];

    const edgeKey = `${ from_node }-${ to_node }`;

    const okEdge = from_node !== to_node;
    const hasEdge = graph.hasEdge(edgeKey);

    const edgeIsOk = okEdge && hasEdge;

    const [isOk, bads] = a;

    return [
      isOk && edgeIsOk,
      !edgeIsOk ? [...bads, edgeKey] : bads
    ]
  }, [nodes.length > 1, []]);
}

const rankEdgeCandidate = (candidateEdge, currentEdge, direction) => {
  const candidateEdgeBearing = candidateEdge.bearing;
  const currentEdgeBearing = currentEdge.bearing;
  const bearingDiff = Math.abs(currentEdgeBearing - candidateEdgeBearing);
  const bearingValue = BEARING_SCALE(bearingDiff);

  const candidateEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[candidateEdge.highway] || 6.0;
  const currentEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[currentEdge.highway] || 6.0;
  const fSystemValue = MAX_F_SYSTEM_RANK - Math.abs(candidateEdgeFsystem - currentEdgeFsystem) * (MAX_F_SYSTEM_RANK / 6.0);

  const wayIdBonus = ((bearingValue + fSystemValue) >= 240 * 0.9) &&
                      (currentEdge.way_id === candidateEdge.way_id) &&
                      ((currentEdge.pos + direction) === candidateEdge.pos) ? 2 : 1

  return (bearingValue + fSystemValue) * wayIdBonus;
}

async function runCheckpoint(initialize, TheConflationator) {
  const [
    getNewNodeId,
    queryResultsSql,
    querySegmentsSql,
    insertFinalResultStmt,
    insertConflationStmt,
    insertProblemRoadStmt,
    insertNodeStmt,
    insertEdgeStmt,
    dropEdgeStmt,
    graph, pathfinder,
    reportStats = NO_OP,
    updatePrevNetwork = NO_OP
  ] = await initialize(TheConflationator);

  const results = TheConflationator.db.all(queryResultsSql);

  const indexSorter = group => group.sort((a, b) => +a.road_index - +b.road_index);
  const resultRollups = d3rollups(results, indexSorter, r => r.road_id, r => r.ls_index);

  TheConflationator.logInfo("PROCESSING", D3_INT_FORMAT(results.length), "RESULTS FROM", D3_INT_FORMAT(resultRollups.length), "ROADWAYS");

  const segments = TheConflationator.db.all(querySegmentsSql)
                      .map(road => ({ ...road,
                                      linestring: JSON.parse(road.geojson).coordinates
                                  })
                      );

  const segmentsGrouped = d3group(segments, r => r.road_id, r => r.ls_index);

  const resultIncAmt = 25000;
  let logResultInfoAt = resultIncAmt;
  let numResults = 0;

  const roadsIncAmt = 50000;
  let logRoadInfoAt = roadsIncAmt;
  let numRoads = 0;

  const logTimerInfoAt = 60000;
  let timestamp = Date.now();

  const newNodes = [];
  const newEdges = [];
  const oldEdges = new Map();

  for (const [road_id, multilinestrings] of resultRollups) {

    const Geometry = {
      type: "MultiLineString",
      coordinates: []
    }

    for (const [lsIndex, lsSegments] of multilinestrings) {

      const osmNodes = filterUnwantedEdges(lsSegments, graph, pathfinder);

// CHECK FOR NOT-EXISTANT EDGES
      const [isOk, badEdges] = checkNodes(osmNodes, graph);

      if (!isOk) {
        insertProblemRoadStmt.run(road_id, "bad-nodes");
      }
      else {

        const roadSegments = segmentsGrouped.get(road_id).get(lsIndex);
        const roadLinestring = roadSegments.reduce((a, c, i) => {
          if (i === 0) {
            return [...c.linestring];
          }
          return [
            ...a,
            ...c.linestring.slice(1)
          ]
        }, []);

        const osmEdges = osmNodes.reduce((a, c, i, osmNodes) => {
          if (i === 0) return a;
          const from_node = osmNodes[i - 1];
          const to_node = osmNodes[i];
          const edgeKey = `${ from_node }-${ to_node }`;
          a.push(graph.getEdgeAttributes(edgeKey));
          return a;
        }, []);

        let roadStartNodeCoords = null;
        let roadStartNodeId = null;

        while (!roadStartNodeCoords) {
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

          const roadStartCoords = findClosestPoint(osmFromNode, osmToNode, roadLinestring, LOOK_BEHIND);
          const roadStartPoint = {
            type: "Point",
            coordinates: roadStartCoords
          };

          const roadPointOnOsmEdge = turf.nearestPointOnLine(osmEdgeGeom, roadStartCoords, { units: "miles" }).geometry;
          const roadCoordsOnOsmEdge = roadPointOnOsmEdge.coordinates;

          const fromDist = turf.distance(osmFromCoords, roadCoordsOnOsmEdge, { units: "miles" });
          const toDist = turf.distance(osmToCoords, roadCoordsOnOsmEdge, { units: "miles" });
          
          if (turf.booleanEqual(osmFromPoint, roadStartPoint)) {
            roadStartNodeCoords = [...osmFromCoords];
            roadStartNodeId = from_node;
          }
          else if (turf.booleanEqual(osmToPoint, roadStartPoint)) {
            roadStartNodeCoords = [...osmToCoords];
            roadStartNodeId = to_node;
          }
          else if (turf.booleanEqual(osmFromPoint, roadPointOnOsmEdge)) {
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
              roadStartNodeCoords = [...osmFromCoords];
              roadStartNodeId = from_node;
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

              const roadPointOnTopInboundEdge = turf.nearestPointOnLine(osmEdgeGeom, roadStartCoords).geometry;

              if (turf.booleanEqual(osmToPoint, roadPointOnTopInboundEdge)) {
                roadStartNodeCoords = [...osmToCoords];
                roadStartNodeId = topInboundEdge.to_node;
              }
              else {
                osmEdges.unshift(topInboundEdge);
              }
            }
          }
          else if (turf.booleanEqual(osmToPoint, roadPointOnOsmEdge)) {
            if (osmEdges.length > 1) {
              osmEdges.shift();
            }
            else {
              roadStartNodeCoords = [...osmToCoords];
              roadStartNodeId = to_node;
            }
          }
          else if (fromDist <= DISTANCE_THRESHOLD) {
            roadStartNodeCoords = [...osmFromCoords];
            roadStartNodeId = from_node;
          }
          else if (toDist <= DISTANCE_THRESHOLD) {
            roadStartNodeCoords = [...osmToCoords];
            roadStartNodeId = to_node;
          }
          else {
            roadStartNodeCoords = [...roadCoordsOnOsmEdge];
            roadStartNodeId = null;
          }
        } // END while

        let roadEndNodeCoords = null;
        let roadEndNodeId = null;

        while (!roadEndNodeCoords) {
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

          const roadEndCoords = findClosestPoint(osmFromNode, osmToNode, roadLinestring, LOOK_AHEAD);
          const roadEndPoint = {
            type: "Point",
            coordinates: roadEndCoords
          };

          const roadPointOnOsmEdge = turf.nearestPointOnLine(osmEdgeGeom, roadEndCoords).geometry;
          const roadCoordsOnOsmEdge = roadPointOnOsmEdge.coordinates;

          const fromDist = turf.distance(osmFromCoords, roadCoordsOnOsmEdge, { units: "miles" });
          const toDist = turf.distance(osmToCoords, roadCoordsOnOsmEdge, { units: "miles" });

          if (turf.booleanEqual(osmFromPoint, roadEndPoint)) {
            roadEndNodeCoords = [...osmFromCoords];
            roadEndNodeId = from_node;
          }
          else if (turf.booleanEqual(osmToPoint, roadEndPoint)) {
            roadEndNodeCoords = [...osmToCoords];
            roadEndNodeId = to_node;
          }
          else if (turf.booleanEqual(osmFromPoint, roadPointOnOsmEdge)) {
            if (osmEdges.length > 1) {
              osmEdges.pop();
            }
            else {
              roadEndNodeCoords = [...osmFromCoords];
              roadEndNodeId = from_node;
            }
          }
          else if (turf.booleanEqual(osmToPoint, roadPointOnOsmEdge)) {
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
              roadEndNodeCoords = [...osmToCoords];
              roadEndNodeId = to_node;
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
              const roadPointOnTopOutboundEdge = turf.nearestPointOnLine(osmEdgeGeom, roadEndCoords).geometry;

              if (turf.booleanEqual(osmFromPoint, roadPointOnTopOutboundEdge)) {
                roadEndNodeCoords = [...osmFromCoords];
                roadEndNodeId = topOutboundEdge.from_node;
              }
              else {
                osmEdges.push(topOutboundEdge);
              }
            }
          }
          else if (fromDist <= DISTANCE_THRESHOLD) {
            roadEndNodeCoords = [...osmFromCoords];
            roadEndNodeId = from_node;
          }
          else if (toDist <= DISTANCE_THRESHOLD) {
            roadEndNodeCoords = [...osmToCoords];
            roadEndNodeId = to_node;
          }
          else {
            roadEndNodeCoords = [...roadCoordsOnOsmEdge];
            roadEndNodeId = null;
          }
        } // END while

        const newOsmNodes = osmEdges.reduce((a, c, i, osmEdges) => {

          const { from_node, to_node } = c;

          const fromNode = graph.getNodeAttributes(from_node);
          const fromCoords = [fromNode.lon, fromNode.lat];

          const toNode = graph.getNodeAttributes(to_node);
          const toCoords = [toNode.lon, toNode.lat];

          if (i === 0) {
            if (roadStartNodeId == from_node) {
              a.push(from_node, to_node);
            }
            else if (roadStartNodeId == to_node) {
              a.push(to_node);
            }
            else {
              // oldEdges.add(`${ from_node }-${ to_node }`);

              const newNodeId = getNewNodeId();
              const newNode = {
                node_id: newNodeId,
                lon: roadStartNodeCoords[0],
                lat: roadStartNodeCoords[1]
              };
              graph.addNode(newNodeId, newNode);
              newNodes.push(newNode);

              const newEdge1 = {
                ...c,
                from_node,
                to_node: newNodeId,
                length: turf.distance(fromCoords, roadStartNodeCoords, { units: "miles" }),
                bearing: turf.bearing(fromCoords, roadStartNodeCoords)
              };
              const edgeKey1 = `${ from_node }-${ newNodeId }`;
              graph.addEdgeWithKey(edgeKey1, from_node, newNodeId, newEdge1);
              newEdges.push(newEdge1);

              // const nextPos = Math.floor(c.pos) + 1.0;

              const newEdge2 = {
                ...c,
                from_node: newNodeId,
                to_node,
                // pos: c.pos + (nextPos - c.pos) * 0.5,
                pos: c.pos + 0.1,
                length: turf.distance(roadStartNodeCoords, toCoords, { units: "miles" }),
                bearing: turf.bearing(roadStartNodeCoords, toCoords)
              };
              const edgeKey2 = `${ newNodeId }-${ to_node }`;
              graph.addEdgeWithKey(edgeKey2, newNodeId, to_node, newEdge2);
              newEdges.push(newEdge2);

              const oldEdgeKey = `${ from_node }-${ to_node }`;
              oldEdges.set(oldEdgeKey, [from_node, newNodeId, to_node]);

              a.push(newNodeId, to_node);
            }
          }
          else if (i === (osmEdges.length - 1)) {
            if (roadEndNodeId == from_node) {
              if (!a.includes(from_node)) {
                a.push(from_node);
              }
            }
            else if (roadEndNodeId == to_node) {
              a.push(to_node);
            }
            else {
              // oldEdges.add(`${ from_node }-${ to_node }`);

              const newNodeId = getNewNodeId();
              const newNode = {
                node_id: newNodeId,
                lon: roadEndNodeCoords[0],
                lat: roadEndNodeCoords[1]
              };
              graph.addNode(newNodeId, newNode);
              newNodes.push(newNode);

              const newEdge1 = {
                ...c,
                from_node,
                to_node: newNodeId,
                length: turf.distance(fromCoords, roadEndNodeCoords, { units: "miles" }),
                bearing: turf.bearing(fromCoords, roadEndNodeCoords)
              };
              const edgeKey1 = `${ from_node }-${ newNodeId }`;
              graph.addEdgeWithKey(edgeKey1, from_node, newNodeId, newEdge1);
              newEdges.push(newEdge1);

              // const nextPos = Math.floor(c.pos) + 1.0;

              const newEdge2 = {
                ...c,
                from_node: newNodeId,
                to_node,
                // pos: c.pos + (nextPos - c.pos) * 0.5,
                pos: c.pos + 0.1,
                length: turf.distance(roadEndNodeCoords, toCoords, { units: "miles" }),
                bearing: turf.bearing(roadEndNodeCoords, toCoords)
              };
              const edgeKey2 = `${ newNodeId }-${ to_node }`;
              graph.addEdgeWithKey(edgeKey2, newNodeId, to_node, newEdge2);
              newEdges.push(newEdge2);

              const oldEdgeKey = `${ from_node }-${ to_node }`;
              oldEdges.set(oldEdgeKey, [from_node, newNodeId, to_node]);

              a.push(newNodeId);
            }
          }
          else {
            a.push(to_node);
          }
          return a;
        }, []);

        const [isStillOk, badEdges] = checkNodes(newOsmNodes, graph);

        if (!isStillOk) {
          insertProblemRoadStmt.run(road_id, "bad-nodes");
        }
        else {

          const roadMiles = roadSegments.reduce((a, c) => {
            return a + c.miles;
          }, 0);

          const osmMiles = newOsmNodes.reduce((a, c, i, nodes) => {
            if (i === 0) return a;
            const edgeKey = `${ nodes[i - 1] }-${ nodes[i] }`;
            return a + graph.getEdgeAttribute(edgeKey, "length");
          }, 0);

          const difference = Math.abs(roadMiles - osmMiles);

          const relativeChange = Math.abs((osmMiles - roadMiles) / roadMiles);

          if ((relativeChange > DISTANCE_RELATIVE_CHANGE) ||
              (difference >= DISTANCE_DIFFERENCE)) {
            insertProblemRoadStmt.run(road_id, "bad-length");
          }

          const osmLinestring = newOsmNodes.map(node_id => {
            const { lon, lat } = graph.getNodeAttributes(node_id);
            return [+lon, +lat];
          });

          for (let i = 1; i < newOsmNodes.length; ++i) {
            const from_node = newOsmNodes[i - 1];
            const to_node = newOsmNodes[i];

            insertConflationStmt.run(road_id, i - 1, from_node, to_node);
          }

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
        TheConflationator.logInfo("PROCESSED", D3_INT_FORMAT(numRoads), "ROAD WAYS");
        timestamp = Date.now();
      }
    } // END for (const [lsIndex, lsSegments] of multilinestrings)

    if (multilinestrings.length === Geometry.coordinates.length) {
      const miles = turf.length(Geometry, { units: "miles" });
      insertFinalResultStmt.run(road_id, miles, JSON.stringify(Geometry));
    }
    if (++numRoads >= logRoadInfoAt) {
      TheConflationator.logInfo("PROCESSED", D3_INT_FORMAT(numRoads), "ROAD WAYS");
      logRoadInfoAt += roadsIncAmt;
      timestamp = Date.now();
    }
  }

  await reportStats(TheConflationator);

  TheConflationator.logInfo("UPDATING NODES AND EDGES");
  for (const node of newNodes) {
    insertNodeStmt.run(node);
  }
  for (const edge of newEdges) {
    insertEdgeStmt.run(edge);
  }
  for (const [edgeKey, update] of oldEdges.entries()) {
    const [from_node, to_node] = edgeKey.split("-");
    dropEdgeStmt.run(from_node, to_node);
    updatePrevNetwork(update);
  }
}
export default runCheckpoint;