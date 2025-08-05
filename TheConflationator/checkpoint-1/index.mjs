
async function runCheckpointOne(TheConflationator) {
  TheConflationator.logInfo("SKIPPING CHECKPOINT ONE");
  return;

  TheConflationator.logInfo("RUNNING CHECKPOINT ONE");

  const [
    nodeInsertStmt,
    edgeInsertStmt,
    edgeDeleteStmt,
    selectNodeStmt
  ] = await initializeCheckpointOne();

  const maxNodeIdSql = `
    SELECT MAX(node_id) AS max_node_id
      FROM nodes;
  `;
  const { max_node_id } = TheConflationator.db.get(maxNodeIdSql);
  let node_id = +max_node_id;
  const getNewNodeId = () => {
    return ++node_id;
  }

  const intersectionNodesSql = `
    SELECT DISTINCT nodes.node_id, lon, lat
      FROM nodes
        JOIN intersections
          USING(node_id);
  `;
  TheConflationator.logInfo("QUERYING INTERSECTION NODES");
  const intersectionNodes = TheConflationator.db.all(intersectionNodesSql);
  TheConflationator.logInfo("RECEIVED", d3intFormat(intersectionNodes.length), "INTERSECTION NODES");

  const intersectionNodesSet = intersectionNodes.reduce((a, c) => {
    return a.add(c.node_id);
  }, new Set());

// HANDLE INTERSECTIONS
// (INTERSECTIONS ARE NODES WHERE MORE THAN 2 WAY IDs MEET)
  const intersectionEdgesSql = `
   SELECT edges.*,
        'incoming' AS dir
      FROM edges
        WHERE edges.to_node = $node_id

    UNION

    SELECT edges.*,
        'outgoing' AS dir
      FROM edges
        WHERE edges.from_node = $node_id
  `;
  const intersectionEdgesStmt = TheConflationator.db.prepare(intersectionEdgesSql);

  const nodeIncAmt = 10000;
  let logNodeInfoAt = nodeIncAmt;
  let numIntersections = 0;

  TheConflationator.logInfo("PROCESSING INTERSECTIONS");
  for (const iNode of intersectionNodes) {
    const { node_id } = iNode;

    const intersectionEdges = intersectionEdgesStmt.all({ node_id });
    const groupedIntersections = d3group(intersectionEdges, e => e.dir, e => e.dir === "incoming" ? e.to_node : e.from_node);

    const incomingIntersectionEdgesMap = groupedIntersections.get("incoming");
    const outgoingIntersectionEdgesMap = groupedIntersections.get("outgoing");

    if (!(incomingIntersectionEdgesMap && outgoingIntersectionEdgesMap)) continue;

    const incomingEdges = incomingIntersectionEdgesMap.get(node_id);
    const outgoingEdges = outgoingIntersectionEdgesMap.get(node_id);

    const hasReversed = [
      ...incomingEdges,
      ...outgoingEdges
    ].reduce((a, c) => {
      return a || Boolean(c.reversed);
    }, false);

// IF THERE ARE NO REVERSED EDGES, THEN ALL EDGES ARE ONE-WAY.
// SINCE A U-TURN IS IMPOSSIBLE, INTERSECTIONS OF ALL ONE-WAY EDGES CAN BE IGNORED.
    if (!hasReversed) continue;

// THERE EXISTS NODES WITH ALL INCOMING EDGES OR ALL OUTGOING EDGES.
// CURRENTLY, I IGNORE THEM.
    if (incomingEdges && outgoingEdges) {

// CREATE NEW INCOMING NODES AND EDGES
      const newIncomingEdges = incomingEdges.map((inEdge, i) => {
        // if (i === 0) {
        //   return inEdge;
        // }

        const newNodeId = getNewNodeId();

        const newNode = {
          ...iNode,
          node_id: newNodeId,
          base_node_id: node_id
        };
        nodeInsertStmt.run(newNode);

        const newEdge = {
          ...inEdge,
          to_node: newNodeId
        };
        edgeDeleteStmt.run(inEdge);
        edgeInsertStmt.run(newEdge);

        return newEdge;
      });

// CREATE NEW OUTGOING NODES AND EDGES
      const newOutgoingEdges = outgoingEdges.map(outEdge => {

        const newNodeId = getNewNodeId();

        const newNode = {
          ...iNode,
          node_id: newNodeId,
          base_node_id: node_id
        };
        nodeInsertStmt.run(newNode);

        const newEdge = {
          ...outEdge,
          from_node: newNodeId
        };
        edgeDeleteStmt.run(outEdge);
        edgeInsertStmt.run(newEdge);

        return newEdge;
      });

// CONNECT NEW INCOMING EDGES TO NEW OUTGOING EDGES
// (EXCEPT FOR IT'S OWN REVERSED OUTGOING EDGE)
      for (const inEdge of newIncomingEdges) {
        for (const outEdge of newOutgoingEdges) {
          if (!((inEdge.way_id === outEdge.way_id) && (outEdge.reversed === 1))) {
            const newEdge = {
              ...inEdge,
              from_node: inEdge.to_node,
              to_node: outEdge.from_node,
              length: 0
            }
            edgeInsertStmt.run(newEdge);
          }
        }
      }

      if (++numIntersections >= logNodeInfoAt) {
        TheConflationator.logInfo("PROCESSED", d3intFormat(numIntersections), "INTERSECTIONS");
        logNodeInfoAt += nodeIncAmt;
      }
    }
  }
  TheConflationator.logInfo("PROCESSED", d3intFormat(numIntersections),"INTERSECTIONS NODES");

// HANDLE REVERSED WAYS
  // const getReversedEdgesSql = `
  //   SELECT *
  //     FROM edges
  //     WHERE reversed = 1
  // `;
  // TheConflationator.logInfo("QUERYING FOR REVERSED EDGES");
  // const reversedEdges = TheConflationator.db.all(getReversedEdgesSql);
  // TheConflationator.logInfo("RECEIVED", d3intFormat(reversedEdges.length), "REVERSED EDGES");

  // const sorter = group => group.sort((a, b) => +a.pos - +b.pos);
  // const rollups = d3rollups(reversedEdges, sorter, e => e.way_id).filter(([way_id, way]) => way.length > 1);
  // TheConflationator.logInfo("PROCESSING", d3intFormat(rollups.length), "REVERSED WAYS");

  // const wayIncAmt = 50000;
  // let logWayInfoAt = wayIncAmt;
  // let numWays = 0;

  // for (const [way_id, way] of rollups) {
  //   const nodeIds = way.reduce((a, c, i) => {
  //     if (i === 0) {
  //       a.push(c.from_node);
  //     }
  //     a.push(c.to_node);
  //     return a;
  //   }, []);

  //   const newNodeIds = nodeIds.map(node_id => {
  //     const oldNode = selectNodeStmt.get(node_id);
  //     if (intersectionNodesSet.has(node_id)) {
  //       return node_id;
  //     }
  //     else if (intersectionNodesSet.has(oldNode.base_node_id)) {
  //       return node_id;
  //     }
  //     else {
  //       const newNodeId = getNewNodeId();
  //       const newNode = {
  //         ...oldNode,
  //         node_id: newNodeId,
  //         base_node_id: node_id
  //       }
  //       nodeInsertStmt.run(newNode);
  //       return newNodeId;
  //     }
  //   })

  //   for (const edge of way) {
  //     edgeDeleteStmt.run(edge);
  //   }

  //   for (let i = 1; i < newNodeIds.length; ++i) {
  //     const from_node = newNodeIds[i - 1];
  //     const fromNode = selectNodeStmt.get(from_node);
  //     const from = [fromNode.lon, fromNode.lat];

  //     const to_node = newNodeIds[i];
  //     const toNode = selectNodeStmt.get(to_node);
  //     const to = [toNode.lon, toNode.lat];

  //     let bearing = turf.bearing(from, to);

  //     const length = turf.distance(from, to, { units: "miles" });

  //     edgeInsertStmt.run({
  //       way_id,
  //       pos: i - 1,
  //       from_node,
  //       to_node,
  //       bearing,
  //       length,
  //       highway: way[i - 1].highway,
  //       reversed: 1
  //     });
  //   }

  //   if (++numWays >= logWayInfoAt) {
  //     TheConflationator.logInfo("PROCESSED", d3intFormat(numWays), "REVERSED WAYS");
  //     logWayInfoAt += wayIncAmt;
  //   }
  // }
  // TheConflationator.logInfo("PROCESSED", d3intFormat(numWays),"REVERSED WAYS");

// REMOVE ANY NODES NOT USED BY EDGES
  const deleteOldNodesSql = `
    WITH node_ids AS (
      SELECT from_node AS node_id
        FROM edges

      UNION

      SELECT to_node AS node_id
        FROM edges
    )
    DELETE FROM nodes
      WHERE node_id NOT IN (
        SELECT DISTINCT node_id FROM node_ids
      )
      RETURNING node_id;
  `;
  TheConflationator.logInfo("DELETING OLD NODES");
  const deletedNodes = TheConflationator.db.all(deleteOldNodesSql);
  TheConflationator.logInfo("DELETED", d3intFormat(deletedNodes.length), "NODES FROM SQLITE DB");

  await reportStatsForCheckpointOne();
}
export default runCheckpointOne;

function initializeCheckpointOne(TheConflationator) {
  TheConflationator.logInfo("INITIALIZING SQLITE DB FOR CHECKPOINT ONE");

  const nodeInsertSql = `
    INSERT INTO nodes(node_id, lon, lat, base_node_id)
      VALUES($node_id, $lon, $lat, $base_node_id);
  `;
  const nodeInsertStmt = TheConflationator.db.prepare(nodeInsertSql);

  const edgeInsertSql = `
    INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, highway, reversed, length, bearing)
      VALUES($way_id, $pos, $from_node, $to_node, $highway, $reversed, $length, $bearing)
  `;
  const edgeInsertStmt = TheConflationator.db.prepare(edgeInsertSql);

  const edgeDeleteSql = `
    DELETE FROM edges 
      WHERE (way_id, from_node, to_node) = ($way_id, $from_node, $to_node);
  `;
  const edgeDeleteStmt = TheConflationator.db.prepare(edgeDeleteSql);

  const selectNodeSql = "SELECT * FROM nodes WHERE node_id = ?;";
  const selectNodeStmt = TheConflationator.db.prepare(selectNodeSql);

  return [
    nodeInsertStmt,
    edgeInsertStmt,
    edgeDeleteStmt,
    selectNodeStmt
  ]
}
async function reportStatsForCheckpointOne() {

}