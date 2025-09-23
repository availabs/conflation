
let counter = 0;
const getUniqueId = () => `unique-id-${ counter++ }`;

export class Rect {
  constructor(xmin, ymin, xmax, ymax) {
    this.xmin = xmin; // LEFT
    this.ymin = ymin; // BOTTOM
    this.ymax = ymax; // TOP
    this.xmax = xmax; // RIGHT

    this.id = getUniqueId();
  }

  static SLOP = Number.EPSILON * 2;

  intersects(rect) {
    if (this.xmax - Rect.SLOP <= rect.xmin) return false;
    if (this.xmin + Rect.SLOP >= rect.xmax) return false;
    if (this.ymin - Rect.SLOP <= rect.ymax) return false;
    if (this.ymax + Rect.SLOP >= rect.ymin) return false;
    return true;
  }
}

async function runCheckpointTwo_NEW(TheConflationator) {
  const queryBoundsSql = `
    SELECT MIN(lon) AS xmin,
            MIN(lat) AS ymin,
            MAX(lon) AS xmax,
            MAX(lat) AS ymax
      FROM nodes;
  `;
  const { xmin, ymin, xmax, ymax } = TheConflationator.db.get(queryBoundsSql);

  const GRIDS_PER_SIDE = 16;

  const wSize = Math.abs(xmin - xmax) / GRIDS_PER_SIDE;
  const hSize = Math.abs(ymin - ymax) / GRIDS_PER_SIDE;

  const queryTmcSegmentsSql = `
    SELECT *
      FROM tmcs
        ORDER BY tmc,
                  linestring_index ASC,
                  tmc_index ASC;
  `;
  const segments = TheConflationator.db.all(queryTmcSegmentsSql)
                      .map(segment => {
                        const geometry = JSON.parse(segment.geojson);
                        const rect = new Rect(...turf.bbox(geometry));
                        return { ...segment, geometry, rect, id: getUniqueId() };
                      });

  const tmcSet = segments.reduce((a, c) => {
    return a.add(c.tmc);
  }, new Set());
  TheConflationator.logInfo("PROCESSING", D3_INT_FORMAT(segments.length), "TMC SEGMENTS DERIVED FROM", D3_INT_FORMAT(tmcSet.size), "TMCs");

  const gridRects = [];

  for (let h = 0; h < GRIDS_PER_SIDE; ++h) {
    for (let w = 0; w < GRIDS_PER_SIDE; ++w) {
      const grid = [
        xmin + wSize * w,
        ymin + hSize * h,
        xmin + wSize + wSize * w,
        ymin + hSize + hSize * h
      ]
      gridRects.push(new Rect(...grid));
    }
  }

  const gridsAndSegmentsMap = new Map();
  const placedSegments = new Set();

  for (const rect of gridRects) {
    for (const segment of segments) {
      if (placedSegments.has(segment.id)) continue;
      if (rect.intersects(segment.rect)) {
        if (!gridsAndSegmentsMap.has(rect.id)) {
          gridsAndSegmentsMap.set(rect.id, { rect, segments: [] });
        }
        gridsAndSegmentsMap.get(rect.id).segments.push(segment);
        placedSegments.add(segment.id)
      }
    }
  }

  const segmentIncAmt = 10000;
  let logSegmentInfoAt = segmentIncAmt;
  let numSegments = 0;

  const logTimerInfoAt = 30000;
  let timestamp = Date.now();

  for (const { rect, segments } of gridsAndSegmentsMap.values()) {

    const { xmin, ymin, xmax, ymax } = rect;

    const gridGeom = {
      type: "Polygon",
      coordinates: [
        [ 
          [xmin, ymin],
          [xmax, ymin],
          [xmax, ymax],
          [xmin, ymax],
          [xmin, ymin]
        ]
      ]
    }

    const bufferedGridGeom = turf.buffer(gridGeom, 50.0, { units: "miles" }).geometry;
    const bufferedBbox = turf.bbox(bufferedGridGeom);

    const [
      quadtree,
      graph,
      pathfinder,
      insertResultStmt
    ] = await initializeCheckpointTwo_NEW(TheConflationator, bufferedBbox);

    for (const segment of segments) {
      const {
        tmc, linestring_index, tmc_index,
        miles, f_system, geometry
      } = segment;

      const linestring = [...geometry.coordinates];

      const bearingLimit = 45.0;

      const seEdge = linestring.slice(0, 2);
      const startEdge = {
        tmc, f_system,
        edge: seEdge,
        bearing: turf.bearing(...seEdge),
        length: turf.distance(...seEdge, { units: "miles" })
      }

      const eeEdge = linestring.slice(-2);
      const endEdge = {
        tmc, f_system,
        edge: eeEdge,
        bearing: turf.bearing(...eeEdge),
        length: turf.distance(...eeEdge, { units: "miles" })
      }

      const topStartCandidateEdges = gatherEdges(startEdge, quadtree);
      const topEndCandidateEdges = gatherEdges(endEdge, quadtree);

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
      if ((Date.now() - timestamp) >= logTimerInfoAt) {
        TheConflationator.logInfo("I'M AAALIIIIIIVE!!!")
        TheConflationator.logInfo("PROCESSED:", D3_INT_FORMAT(numSegments), "TMC SEGMENTS");
        timestamp = Date.now();
      }
    }
  }
  
  await reportStatsForCheckpointTwo(TheConflationator);
}

async function initializeCheckpointTwo_NEW(TheConflationator, bbox) {
  const [xmin, ymin, xmax, ymax] = bbox;

  const graph = new MultiDirectedGraph({ allowSelfLoops: false });

  let incAmt = 100000;
  let logInfoAt = incAmt;
  let numNodes = 0;

  TheConflationator.logInfo("LOADING NODES INTO GRAPH");
  const queryNodesSql = `
    SELECT node_id, lon, lat
      FROM nodes
        WHERE lon >= ${ xmin }
        AND lat >= ${ ymin }
        AND lon <= ${ xmax }
        AND lat <= ${ ymax }
  `;
  const nodesIterator = TheConflationator.db.prepare(queryNodesSql).iterate();
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
  const queryEdgesSql = `
    WITH node_ids AS (
      SELECT node_id
        FROM nodes
          WHERE lon >= ${ xmin }
          AND lat >= ${ ymin }
          AND lon <= ${ xmax }
          AND lat <= ${ ymax }
      )

    SELECT * FROM edges
      WHERE from_node IN (
        SELECT * from node_ids
      )
  `;
  const edgesIterator = TheConflationator.db.prepare(queryEdgesSql).iterate();
  for (const edge of edgesIterator) {
    const { from_node, to_node, highway } = edge;

    if (!graph.hasNode(to_node)) continue;

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

    const divLength = HIGHWAY_TO_DIV_LENGTH_MAP[highway];
    const divs = Math.ceil(edge.length / divLength) - 1;
    const oneDivth = edge.length / divs;

    for (let i = 1; i < divs; ++i) {
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
    CREATE TABLE IF NOT EXISTS results(
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