// import { scaleLinear as d3scaleLinear } from "d3-scale"

// import * as turf from "@turf/turf";

// import {
//   MIN_MILES_TO_RANK,
//   MAX_MILES_RANK,
//   BEARING_SCALE,
//   HIGHWAY_TO_F_SYSTEM_MAP,

//   MAX_F_SYSTEM_RANK,

//   BUFFER_SCALE,

//   NUM_EDGES_TO_KEEP,

//   MAX_NUM_GROWTHS,
//   GROW_BACKWARD,
//   GROW_FORWARD,
//   GROWTH_AMOUNT,

//   NUM_GAHTER_PASSES,
//   QUALITY_RANK_FILTER
// } from "./constants.mjs"

// const rankEdgeCandidate = (candidateEdge, edge) => {
//   const candidateEdgeBearing = candidateEdge.bearing;
//   const edgeBearing = edge.bearing;
//   const bearingDiff = Math.abs(edgeBearing - candidateEdgeBearing);
//   const bearingValue = BEARING_SCALE(bearingDiff);

//   const candidateEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[candidateEdge.highway];
//   const fSystemValue = MAX_F_SYSTEM_RANK - Math.abs(+edge.f_system - candidateEdgeFsystem) * (MAX_F_SYSTEM_RANK / 6);

//   return bearingValue + fSystemValue;
// }

// const findAllWithin = (edge, quadtree, buffer) => {
//   const edgeGeometry = {
//     type: "LineString",
//     coordinates: [...edge.edge]
//   }
//   const buffered = turf.buffer(edgeGeometry, buffer, { units: "miles" });

//   const [xmin, ymin, xmax, ymax] = turf.bbox(buffered);

//   const edgeSet = new Set();

//   quadtree.visit((node, x1, y1, x2, y2) => {
//     if (!node.length) {
//       do {
//         const d = node.data;

//         if (edgeSet.has(d.edge)) continue;

//         const p = {
//           type: "Point",
//           coordinates: [d.lon, d.lat]
//         }
//         const ls = {
//           type: "LineString",
//           coordinates: [...d.edge.edge]
//         }
//         if (turf.booleanContains(buffered, p)) {
//           edgeSet.add(d.edge);
//         }
//         else if (turf.booleanIntersects(buffered, ls)) {
//           edgeSet.add(d.edge);
//         }
//       } while (node = node.next);
//     }
//     return x1 >= xmax || y1 >= ymax || x2 < xmin || y2 < ymin;
//   });

//   return [...edgeSet];
// }

// const growEdge = (edge, growth) => {

//   const growthDirection = growth % 2 === 0 ? GROW_BACKWARD : GROW_FORWARD;

//   const bearing = growthDirection === GROW_FORWARD ?  edge.bearing : edge.bearing < 0.0 ? edge.bearing + 180.0 : edge.bearing - 180.0;
//   const index = growthDirection === GROW_BACKWARD ? 0 : 1;
//   const newPoint = turf.destination(edge.edge[index], GROWTH_AMOUNT, bearing, { unit: "miles" }).geometry.coordinates;

//   const [e1, e2] = edge.edge;

//   const p1 = growthDirection === GROW_BACKWARD ? newPoint : e1;
//   const p2 = growthDirection === GROW_BACKWARD ? e2 : newPoint;

//   return {
//     ...edge,
//     edge: [p1, p2]
//   }
// }

// const getGatheredEdges = (qualityEdges, poorEdges = new Map()) => {
//   return [
//     ...qualityEdges.values(),
//     ...poorEdges.values()
//   ].sort((a, b) => b.rank - a.rank)
//     .slice(0, NUM_EDGES_TO_KEEP);
// }

// export const gatherEdges = (edge, quadtree, qualityEdges = new Map(), poorEdges = new Map(), growth = 0) => {
//   if (growth >= MAX_NUM_GROWTHS) {
//     return getGatheredEdges(qualityEdges, poorEdges);
//   }

//   for (let pass = 1; pass <= NUM_GAHTER_PASSES; ++pass) {
//     const divisor = pass / NUM_GAHTER_PASSES;
//     const buffer = BUFFER_SCALE(+edge.f_system) * divisor;
//     const candidateEdges = findAllWithin(edge, quadtree, buffer);

//     for (const candidateEdge of candidateEdges) {
//       candidateEdge.rank = rankEdgeCandidate(candidateEdge, edge);

//       if (candidateEdge.rank >= QUALITY_RANK_FILTER) {
//         qualityEdges.set(candidateEdge.edgeKey, candidateEdge);
//       }
//       else {
//         poorEdges.set(candidateEdge.edgeKey, candidateEdge);
//       }
//     }
// // RETURN QUALITY EDGES HERE WITH THE NARROW MOST BUFFER
//     if (qualityEdges.size >= NUM_EDGES_TO_KEEP) {
//       return getGatheredEdges(qualityEdges);
//     }
//   }

//   if (qualityEdges.size < NUM_EDGES_TO_KEEP) {
//     return gatherEdges(growEdge(edge, growth), quadtree, qualityEdges, poorEdges, growth + 1);
//   }

//   return getGatheredEdges(qualityEdges, poorEdges);
// }

// export const processPath = (graph, path, miles, sce, ece, results) => {
//   const pathWithNodes = path.map(nid => graph.getNodeAttributes(nid));

//   const calculatedMiles = path.reduce((a, c, i, path) => {
//     if (i === 0) return a;
//     const edgeKey = `${ path[i - 1] }-${ path[i] }`;
//     return a + graph.getEdgeAttribute(edgeKey, "length");
//   }, 0);

//   let milesRank = MAX_MILES_RANK;

//   if (miles >= MIN_MILES_TO_RANK) {
//     milesRank = (Math.min(calculatedMiles, miles) / Math.max(calculatedMiles, miles));
//   }

//   results.push([pathWithNodes, calculatedMiles, sce.rank, ece.rank, milesRank]);
// }