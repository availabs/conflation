import { pipeline, Transform, Readable } from "node:stream";
import { join, dirname, basename } from "node:path";
import { fileURLToPath } from 'node:url';
import {
  existsSync,
  readdirSync,
  copyFileSync,
  rmSync,
  mkdirSync,
  createWriteStream,
  createReadStream
} from "node:fs"

import { dsvFormat as d3dsvFormat } from "d3-dsv"
import { format as d3format } from "d3-format"
import { quadtree as d3quadtree } from "d3-quadtree"
import {
  group as d3group,
  rollup as d3rollup,
  rollups as d3rollups
} from "d3-array"
import { scaleLinear as d3scaleLinear } from "d3-scale"

import * as turf from "@turf/turf";

import split2 from "split2"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import { MultiDirectedGraph } from "graphology"
import { dijkstra } from 'graphology-shortest-path';

import SQLite3DB from "./BetterSQLite3DB.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const WORKING_DIRECTORY = join(__dirname, "sqlite");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

const CHECKPOINTS_FOLDER = join(__dirname, "checkpoints");
const CHECKPOINT_REGEX = /^checkpoint[-]\d+.sqlite$/;

const HIGHWAY_TO_F_SYSTEM_MAP = {
  motorway: 1.0,
  motorway_link: 1.5,

  trunk: 2.0,
  trunk_link: 2.5,

  primary: 3.0,
  primary_link: 3.5,

  secondary: 4.0,
  secondary_link: 4.5,

  tertiary: 5.0,
  tertiary_link: 5.5,

  unclassified: 6.0,

  residential: 7.0,
  living_street: 7.0
}

const MAX_BEARING_RANK = 120.0;
const MAX_F_SYSTEM_RANK = 120.0;
const MAX_MILES_RANK = 60.0;
const MIN_MILES_TO_RANK = 0.1;

const BEARING_SCALE = d3scaleLinear([0.0, 90.0, 180.0], [MAX_BEARING_RANK, -MAX_BEARING_RANK, -2.0 * MAX_BEARING_RANK]);

const d3intFormat = d3format(",d");
const d3dsvFormatter = d3dsvFormat("|");

const COERCES_TO_NUMBERS = ["", null, true, false, []]
const strictNaN = value => {
  return COERCES_TO_NUMBERS.includes(value) ?  true : isNaN(value);
}

// THESE MULTILINESTRINGS ARE UNUSABLE IN THEIR CURRENT FORM
const UNUSABLE_MULTILINESTRING_SET = new Set([
  "120P31240",
  "120P29310",
  "120N10878",
  "120P27870",
  "120P15529",
  "120N12285",
  "104P06033",
  "104P11799",
  "104P11993",
  "104P11917",
  "104P14957",
  "104N06779"
]);

export default class TheConflationator {
  constructor(config) {
    this.config = { ...config };
    this.checkpoint = null

    this.db = null;
  }
  async finalize() {
    await this.db.close();
    this.removeSqliteDirectory();
  }
  logInfo(...args) {
    const string = args.reduce((a, c) => {
      if (typeof c === "object") {
        return `${ a } ${ JSON.stringify(c) }`;
      }
      return `${ a } ${ c }`
    }, `${ new Date().toLocaleString() }:`);
    console.log(string);
  }
  async getDataTable(client, viewId) {
    const sql = `
      SELECT data_table
        FROM data_manager.views
        WHERE view_id = $1
    `
    const { rows: [{ data_table }]} = await client.query(sql, [viewId]);
    return data_table;
  }
  removeSqliteDirectory() {
    rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
  }
  prepSqliteDirectory() {
    this.removeSqliteDirectory();
    mkdirSync(WORKING_DIRECTORY);
  }
  loadLatestCheckpoint() {
    this.prepSqliteDirectory();
    const checkpoints = readdirSync(CHECKPOINTS_FOLDER)
                          .filter(f => CHECKPOINT_REGEX.test(f))
                          .sort((a, b) => b.localeCompare(a));
    if (checkpoints.length) {
      const sqliteFile = checkpoints[0];
      this.checkpoint = basename(sqliteFile, ".sqlite");
      this.db = this.loadFromDisk(join(CHECKPOINTS_FOLDER, sqliteFile));
      this.logInfo("LOADED CHECKPOINT:", this.checkpoint);
    }
    else {
      this.db = new SQLite3DB(ACTIVE_DB_PATH);
    }
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('cache_size = -4096');
    return this;
  }
  async saveCheckpoint(checkpoint) {
    this.logInfo("SAVING CHECKPOINT", checkpoint, "TO DISK");
    const filepath = join(CHECKPOINTS_FOLDER, `${ checkpoint }.sqlite`);
    await this.saveToDisk(filepath);
  }
  async saveToDisk(filepath) {
    await this.db.run(`VACUUM main INTO '${ filepath }';`);
  }
  loadFromDisk(filepath) {
    if (existsSync(filepath)) {
      this.logInfo("FILE", filepath, "EXISTS. LOADING SQLITE DB.");
      copyFileSync(filepath, ACTIVE_DB_PATH);
    }
    else {
      this.logInfo("FILE", filepath, "DOES NOT EXIST. CREATING NEW SQLITE DB.");
    }
    return new SQLite3DB(ACTIVE_DB_PATH);
  }
  async run() {
    switch (this.checkpoint) {
      case "checkpoint-3":
        return (async () => { this.logInfo("NO CHECKPOINT 4")})();
      case "checkpoint-2":
        return this.runCheckpointThree();
      case "checkpoint-1":
        return this.runCheckpointTwo();
      case "checkpoint-0":
        return this.runCheckpointOne();
      default:
        return this.runCheckpointZero();
    }
  }
  async runCheckpointZero() {
    this.logInfo("RUNNING CHECKPOINT ZERO");

    const { npmrds2, OSM_view_id, NPMRDS_view_id } = this.config;

    this.logInfo("CONNECTING CLIENT");
    const client = new pgStuff.Client(npmrds2);
    await client.connect();

    const [
      node_insert_stmt,
      edge_insert_stmt,
      tmc_insert_stmt
    ] = this.initializeCheckpointZero();

    const OSM_ways_table = await this.getDataTable(client, OSM_view_id);
    const OSM_nodes_table = `${ OSM_ways_table }_nodes`;
    const NPMRDS_table = await this.getDataTable(client, NPMRDS_view_id);

    await this.loadTmcs(client, NPMRDS_table, tmc_insert_stmt);

    await this.loadNodes(client, OSM_nodes_table, node_insert_stmt);
    const maxNodeIdSql = `
      SELECT MAX(node_id) AS max_node_id
        FROM nodes;
    `;
    const { max_node_id } = this.db.get(maxNodeIdSql);
    let node_id = +max_node_id;
    const getNewNodeId = () => {
      return ++node_id;
    }
    await this.loadWays(client, OSM_ways_table, edge_insert_stmt, getNewNodeId, node_insert_stmt);

    await client.end();

    this.findIntersections();

    this.reportStatsForCheckpointZero();

    await this.saveCheckpoint("checkpoint-0");

    return this.runCheckpointOne();
  }
  initializeCheckpointZero() {
    this.logInfo("INITIALIZING SQLITE DB FOR CHECKPOINT ZERO");

    this.db.run(`
      CREATE TABLE IF NOT EXISTS nodes(
        node_id INT PRIMARY KEY,
        lon DOUBLE PRECISION NOT NULL,
        lat DOUBLE PRECISION NOT NULL,
        base_node_id INT
      );
    `);
    const node_insert_stmt = this.db.prepare("INSERT INTO nodes(node_id, lon, lat) VALUES(?, ?, ?);");

    this.db.run(`
      CREATE TABLE IF NOT EXISTS edges(
        way_id INT NOT NULL,
        pos DOUBLE PRECISION NOT NULL,
        from_node INT NOT NULL,
        to_node INT NOT NULL,
        bearing DOUBLE PRECISION,
        length DOUBLE PRECISION,
        highway TEXT,
        reversed INT NOT NULL,
        PRIMARY KEY(from_node, to_node),
        FOREIGN KEY(from_node) REFERENCES nodes(node_id) ON DELETE CASCADE,
        FOREIGN KEY(to_node) REFERENCES nodes(node_id) ON DELETE CASCADE
      );
    `);
    const edge_insert_stmt = this.db.prepare("INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, bearing, length, highway, reversed) VALUES(?, ?, ?, ?, ?, ?, ?, ?);");
    
    const edgesWayIdIndexSql = `
      CREATE INDEX edges_way_id_idx
        ON edges(way_id);
    `;
    this.db.run(edgesWayIdIndexSql);
    
    const edgesFromNodeIndexSql = `
      CREATE INDEX edges_from_node_idx
        ON edges(from_node);
    `;
    this.db.run(edgesFromNodeIndexSql);

    const edgesToNodeIndexSql = `
      CREATE INDEX edges_to_node_idx
        ON edges(to_node);
    `;
    this.db.run(edgesToNodeIndexSql);

    this.db.run(`
      CREATE TABLE IF NOT EXISTS tmcs(
        tmc TEXT,
        linestring_index INT,
        tmc_index INT,
        miles DOUBLE PRECISION,
        f_system INT,
        geojson TEXT,
        PRIMARY KEY(tmc, linestring_index, tmc_index)
      );
    `);
    const tmc_insert_stmt = this.db.prepare("INSERT INTO tmcs(tmc, linestring_index, tmc_index, miles, f_system, geojson) VALUES(?, ?, ?, ?, ?, ?);");

    this.db.run(`
      CREATE TABLE IF NOT EXISTS intersections(
        node_id INT PRIMARY KEY
      );
    `);

    return [
      node_insert_stmt,
      edge_insert_stmt,
      tmc_insert_stmt
    ]
  }
  async reportStatsForCheckpointZero() {
    const nodeCountSql = `
      SELECT COUNT(1) AS count
      FROM nodes
    `;
    const { count: nodes } = this.db.get(nodeCountSql);
    this.logInfo("LOADED", d3intFormat(nodes), "NODES INTO SQLITE DB");

    const edgeCountSql = `
      SELECT COUNT(1) AS count
      FROM edges
    `;
    const { count: edges } = this.db.get(edgeCountSql);
    this.logInfo("LOADED", d3intFormat(edges), "EDGES INTO SQLITE DB");

    const tmcCountSql = `
      SELECT COUNT(1) AS count
      FROM tmcs
    `;
    const { count: tmcs } = this.db.get(tmcCountSql);
    this.logInfo("LOADED", d3intFormat(tmcs), "TMCs INTO SQLITE DB");

    const intersectionCountSql = `
      SELECT COUNT(1) AS count
      FROM intersections
    `;
    const { count: intersections } = this.db.get(intersectionCountSql);
    this.logInfo("LOADED", d3intFormat(intersections), "INTERSECTIONS INTO SQLITE DB");
  }

  async runCheckpointOne() {
    this.logInfo("RUNNING CHECKPOINT ONE");

    await this.reportStatsForCheckpointOne();

    await this.saveCheckpoint("checkpoint-1");

    return this.runCheckpointTwo();

    const [
      nodeInsertStmt,
      edgeInsertStmt,
      edgeDeleteStmt,
      selectNodeStmt
    ] = await this.initializeCheckpointOne();

    const maxNodeIdSql = `
      SELECT MAX(node_id) AS max_node_id
        FROM nodes;
    `;
    const { max_node_id } = this.db.get(maxNodeIdSql);
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
    this.logInfo("QUERYING INTERSECTION NODES");
    const intersectionNodes = this.db.all(intersectionNodesSql);
    this.logInfo("RECEIVED", d3intFormat(intersectionNodes.length), "INTERSECTION NODES");

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
    const intersectionEdgesStmt = this.db.prepare(intersectionEdgesSql);

    const nodeIncAmt = 10000;
    let logNodeInfoAt = nodeIncAmt;
    let numIntersections = 0;

    this.logInfo("PROCESSING INTERSECTIONS");
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
          this.logInfo("PROCESSED", d3intFormat(numIntersections), "INTERSECTIONS");
          logNodeInfoAt += nodeIncAmt;
        }
      }
    }
    this.logInfo("PROCESSED", d3intFormat(numIntersections),"INTERSECTIONS NODES");

// HANDLE REVERSED WAYS
    // const getReversedEdgesSql = `
    //   SELECT *
    //     FROM edges
    //     WHERE reversed = 1
    // `;
    // this.logInfo("QUERYING FOR REVERSED EDGES");
    // const reversedEdges = this.db.all(getReversedEdgesSql);
    // this.logInfo("RECEIVED", d3intFormat(reversedEdges.length), "REVERSED EDGES");

    // const sorter = group => group.sort((a, b) => +a.pos - +b.pos);
    // const rollups = d3rollups(reversedEdges, sorter, e => e.way_id).filter(([way_id, way]) => way.length > 1);
    // this.logInfo("PROCESSING", d3intFormat(rollups.length), "REVERSED WAYS");

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
    //     this.logInfo("PROCESSED", d3intFormat(numWays), "REVERSED WAYS");
    //     logWayInfoAt += wayIncAmt;
    //   }
    // }
    // this.logInfo("PROCESSED", d3intFormat(numWays),"REVERSED WAYS");

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
    this.logInfo("DELETING OLD NODES");
    const deletedNodes = this.db.all(deleteOldNodesSql);
    this.logInfo("DELETED", d3intFormat(deletedNodes.length), "NODES FROM SQLITE DB");

    await this.reportStatsForCheckpointOne();

    await this.saveCheckpoint("checkpoint-1");

    return this.runCheckpointTwo();
  }
  initializeCheckpointOne() {
    this.logInfo("INITIALIZING SQLITE DB FOR CHECKPOINT ONE");

    const nodeInsertStmt = this.db.prepare("INSERT INTO nodes(node_id, lon, lat, base_node_id) VALUES($node_id, $lon, $lat, $base_node_id)");
    const edgeInsertStmt = this.db.prepare("INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, highway, reversed, length, bearing) VALUES($way_id, $pos, $from_node, $to_node, $highway, $reversed, $length, $bearing)");
    const edgeDeleteStmt = this.db.prepare("DELETE FROM edges WHERE (way_id, from_node, to_node) = ($way_id, $from_node, $to_node);");
    const selectNodeStmt = this.db.prepare("SELECT * FROM nodes WHERE node_id = ?;");

    return [
      nodeInsertStmt,
      edgeInsertStmt,
      edgeDeleteStmt,
      selectNodeStmt
    ]
  }
  async reportStatsForCheckpointOne() {

  }

  async runCheckpointTwo() {
    this.logInfo("RUNNING CHECKPOINT TWO");

    const [
      quadtree,
      graph,
      pathfinder,
      insertResultStmt
    ] = await this.initializeCheckpointTwo();


/*
PROBLEM TMCs

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

    const MAX_FEET = 50.0;
    const MIN_FEET = 25.0;
    const FEET_TO_MILES = 1.0 / 5280.0;

    const milesScale = d3scaleLinear([1, 7], [MAX_FEET * FEET_TO_MILES, MIN_FEET * FEET_TO_MILES]);

    const NUM_EDGES_TO_KEEP = 3;
    const NUM_RESULTS_TO_KEEP = 3;

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
      const buffered = turf.buffer(tmcEdgeGeometry, milesScale(+tmcEdge.f_system), { units: "miles" });

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
    const segments = this.db.all(queryTmcsSql)
                        .filter(segment => !UNUSABLE_MULTILINESTRING_SET.has(segment.tmc)) // THESE TMC SHAPES ARE UNUSABLE
                        .map(segment => ({ ...segment, geometry: JSON.parse(segment.geojson) }));

    const tmcSet = segments.reduce((a, c) => {
      return a.add(c.tmc);
    }, new Set());
    this.logInfo("PROCESSING", d3intFormat(segments.length), "TMC SEGMENTS DERIVED FROM", d3intFormat(tmcSet.size), "TMCs");

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
        this.logInfo("PROCESSED:", d3intFormat(numSegments), "TMC SEGMENTS");
        logSegmentInfoAt += segmentIncAmt;
        timestamp = Date.now();
      }
      if (processedTMCs.size >= logTmcInfoAt) {
        this.logInfo("PROCESSED:", d3intFormat(processedTMCs.size), "TMCs");
        logTmcInfoAt += tmcIncAmt;
        timestamp = Date.now();
      }
      if ((Date.now() - timestamp) >= logTimerInfoAt) {
        this.logInfo("I'M AAALIIIIIIVE!!!")
        this.logInfo("PROCESSED:", d3intFormat(numSegments), "TMC SEGMENTS");
        this.logInfo("PROCESSED:", d3intFormat(processedTMCs.size), "TMCs");
        timestamp = Date.now();
      }
    }

    await this.reportStatsForCheckpointTwo();

    await this.saveCheckpoint("checkpoint-2");

    return this.runCheckpointThree(graph);
  }

  async initializeCheckpointTwo() {

    const quadtree = d3quadtree().x(n => n.lon).y(n => n.lat);

    const graph = new MultiDirectedGraph({ allowSelfLoops: false });

    const incAmt = 500000;
    let logInfoAt = incAmt;
    let numNodes = 0;

    this.logInfo("LOADING NODES INTO GRAPH");
    const nodesSql = `
      SELECT node_id, lon, lat
        FROM nodes
    `;
    const nodesIterator = this.db.prepare(nodesSql).iterate();
    for (const node of nodesIterator) {
      graph.addNode(node.node_id, node);
      if (++numNodes >= logInfoAt) {
        this.logInfo("LOADED", d3intFormat(numNodes), "NODES");
        logInfoAt += incAmt;
      }
    }

    logInfoAt = incAmt;
    let numEdges = 0;

    this.logInfo("LOADING EDGES INTO GRAPH AND QUADTREE");
    const edgesIterator = this.db.prepare("SELECT * FROM edges").iterate();
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
        this.logInfo("LOADED", d3intFormat(numEdges), "EDGES");
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
    this.db.run(createResultsTableSql);
    const insertResultSql = `
      INSERT INTO results(tmc, linestring_index, tmc_index, path, rank, start_score, end_score, miles_score, miles)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    `;
    const insertResultStmt = this.db.prepare(insertResultSql);

    return [
      quadtree,
      graph,
      pathfinder,
      insertResultStmt
    ];
  }

  async reportStatsForCheckpointTwo() {
    const numTMCsSql = `
      SELECT COUNT(DISTINCT tmc) as num_tmcs
        FROM tmcs
          WHERE tmc NOT LIKE 'C%';
    `
    const { num_tmcs } = this.db.get(numTMCsSql);

    const numTMCsWithResultsSql = `
      SELECT COUNT(DISTINCT tmc) AS num_tmcs_with_results
        FROM results
    `
    const { num_tmcs_with_results } = this.db.get(numTMCsWithResultsSql);

    this.logInfo("FOUND PATHS FOR", d3intFormat(num_tmcs_with_results), "TMCs OUT OF", d3intFormat(num_tmcs), "TOTAL TMCs");
    this.logInfo("MISSING PATHS FOR", d3intFormat(num_tmcs - num_tmcs_with_results), "TMCs");

    const createProblemTmcsTableSql = `
      CREATE TABLE problem_tmcs(
        tmc TEXT,
        problem TEXT,
        PRIMARY KEY(tmc, problem)
      )
    `;
    this.db.run(createProblemTmcsTableSql);
    this.logInfo("CREATED problem_tmcs TABLE");
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
    this.db.run(insertProblemTmcsSql);
    this.logInfo("INSERTED PROBLEM TMCs INTO TABLE problem_tmcs");
  }

  async runCheckpointThree(graphFromCheckpoint2) {
    this.logInfo("RUNNING CHECKPOINT THREE");

    const [
      getNewNodeId,
      insertFinalResultStmt,
      graph,
      pathfinder,
      insertProblemTmcStmt
    ] = await this.initializeCheckpointThree(graphFromCheckpoint2);


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
    const results = this.db.all(queryResultsSql);

    const tmcIndexSorter = group => group.sort((a, b) => +a.tmc_index - +b.tmc_index);
    const resultRollups = d3rollups(results, tmcIndexSorter, r => r.tmc, r => r.linestring_index);

    this.logInfo("PROCESSING", d3intFormat(results.length), "RESULTS FROM", d3intFormat(resultRollups.length), "TMCs");

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
    const tmcs = this.db.all(queryTmcsSql)
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

          const DISTANCE_THRESHOLD = 5.0 / 5280.0;

          const LOOK_BEHIND = -1;
          const LOOK_AHEAD = 1;

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
          this.logInfo("PROCESSED", d3intFormat(numResults), "RESULTS");
          logResultInfoAt += resultIncAmt;
          timestamp = Date.now();
        }
        if ((Date.now() - timestamp) >= logTimerInfoAt) {
          this.logInfo("I'M AAALIIIIIIVE!!!")
          this.logInfo("PROCESSED", d3intFormat(numResults), "RESULTS");
          this.logInfo("PROCESSED", d3intFormat(numTMCs), "TMCs");
          timestamp = Date.now();
        }
      } // END for (const [lsIndex, lsSegments] of multilinestrings)

      if (multilinestrings.length === Geometry.coordinates.length) {
        const miles = turf.length(Geometry, { units: "miles" });
        insertFinalResultStmt.run(tmc, miles, JSON.stringify(Geometry));
      }
      if (++numTMCs >= logTmcInfoAt) {
        this.logInfo("PROCESSED", d3intFormat(numTMCs), "TMCs");
        logTmcInfoAt += tmcIncAmt;
        timestamp = Date.now();
      }
    } // END for (const [tmc, multilinestrings] of resultRollups)

    await this.reportStatsForCheckpointThree();

    await this.saveCheckpoint("checkpoint-3");
  }
  async reportStatsForCheckpointThree() {
    const numNoPathsSql = `
      SELECT COUNT(1) AS num_no_paths
        FROM problem_tmcs
          WHERE problem = 'no-path'
    `
    const { num_no_paths } = this.db.get(numNoPathsSql);

    const numBadNodesSql = `
      SELECT COUNT(1) AS num_bad_nodes
        FROM problem_tmcs
          WHERE problem = 'bad-nodes'
    `
    const { num_bad_nodes } = this.db.get(numBadNodesSql);

    const numBadLengthsSql = `
      SELECT COUNT(1) AS num_bad_lengths
        FROM problem_tmcs
          WHERE problem = 'bad-length'
    `
    const { num_bad_lengths } = this.db.get(numBadLengthsSql);

    this.logInfo("NUMBER OF TMCs WITH NO PATHS:", d3intFormat(num_no_paths));
    this.logInfo("NUMBER OF TMCs WITH BAD NODES:", d3intFormat(num_bad_nodes));
    this.logInfo("NUMBER OF TMCs WITH BAD LENGTHS:", d3intFormat(num_bad_lengths));
    this.logInfo(d3intFormat(num_no_paths + num_bad_nodes + num_bad_lengths), "TOTAL PROBLEM TMCs");
  }
  async initializeCheckpointThree(graphFromCheckpoint2) {

    const maxNodeIdSql = `
      SELECT MAX(node_id) AS max_node_id
        FROM nodes;
    `;
    const { max_node_id } = this.db.get(maxNodeIdSql);
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
    this.db.run(createFinalResultsTableSql);

    const insertFinalResultSql = `
      INSERT INTO final_results(tmc, miles, wkb_geometry)
        VALUES(?, ?, ?);
    `;
    const insertFinalResultStmt = this.db.prepare(insertFinalResultSql);

    const graph = graphFromCheckpoint2 || new MultiDirectedGraph({ allowSelfLoops: false });

    if (graphFromCheckpoint2) {
      this.logInfo("USING GRAPH FROM CHECKPOINT 2");
    }
    else {
      const incAmt = 500000;
      let logInfoAt = incAmt;
      let numNodes = 0;

      this.logInfo("LOADING NODES INTO GRAPH");
      const nodesIterator = this.db.prepare("SELECT * FROM nodes").iterate();
      for (const node of nodesIterator) {
        graph.addNode(node.node_id, node);
        if (++numNodes >= logInfoAt) {
          this.logInfo("LOADED", d3intFormat(numNodes), "NODES");
          logInfoAt += incAmt;
        }
      }

      logInfoAt = incAmt;
      let numEdges = 0;

      this.logInfo("LOADING EDGES INTO GRAPH");
      const edgesIterator = this.db.prepare("SELECT * FROM edges").iterate();
      for (const edge of edgesIterator) {
        const { from_node, to_node } = edge;
        graph.addEdgeWithKey(`${ from_node }-${ to_node }`, from_node, to_node, edge);
        if (++numEdges >= logInfoAt) {
          this.logInfo("LOADED", d3intFormat(numEdges), "EDGES");
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
    const insertProblemTmcStmt = this.db.prepare(insertProblemTmcSql);

    return [getNewNodeId, insertFinalResultStmt, graph, pathfinder, insertProblemTmcStmt];
  }

  async runCheckpointFour() {
    this.logInfo("RUNNING CHECKPOINT FOUR");

  }
  async initializeCheckpointFour() {

  }
  async reportStatsForCheckpointFour() {

  }

  getNodeTransform(node_insert_stmt) {
    const theConflationator = this;
    const incAmt = 500000;
    let logInfoAt = incAmt;
    let numNodes = 0;
    return new Transform({
      transform(chunk, encoding, next) {
        const [id, lon, lat] = chunk.toString().split("|").map(Number);
        node_insert_stmt.run(id, lon, lat);
        if (++numNodes >= logInfoAt) {
          theConflationator.logInfo("LOADED", d3intFormat(numNodes), "NODES");
          logInfoAt += incAmt;
        }
        next();
      }
    });
  }
  async loadNodes(client, OSM_nodes_table, node_insert_stmt) {
    const copyToStream = client.query(
      pgCopyStreams.to(`
        COPY ${ OSM_nodes_table }(osm_id, lon, lat)
        TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
      `)
    );
    this.logInfo("LOADING NODES");
    await new Promise((resolve, reject) => {
      pipeline(
        copyToStream,
        split2(),
        this.getNodeTransform(node_insert_stmt),
        error => {
          if (error) {
            reject(error);
          }
          else {
            resolve();
          }
        }
      )
    })
  }

  getWayTransform(edge_insert_stmt, getNewNodeId, node_insert_stmt) {
    const theConflationator = this;
    const incAmt = 50000;
    let logInfoAt = incAmt;
    let numWays = 0;
    const selectNodesSql = `
      SELECT *
        FROM nodes
        WHERE node_id IN (__NODE_IDs__)
    `;
    return new Transform({
      transform(chunk, encoding, next) {
        const [parsedRow] = d3dsvFormatter.parseRows(chunk.toString());
        const [id, refs, tags] = parsedRow.map(JSON.parse);

        const nodes = theConflationator.db.all(selectNodesSql.replace("__NODE_IDs__", refs));
        const nodesMap = nodes.reduce((a, c) => { a[c.node_id] = [c.lon, c.lat]; return a; }, {});

        let needsForward = tags.oneway !== "-1";
        let needsReverse = (tags.oneway === "-1") || ((tags.oneway !== "yes") && (tags.highway !== "motorway"));

        if (needsForward) {

          for (let i = 1; i < refs.length; ++i) {

            const from_node = refs[i - 1];
            const from = nodesMap[from_node];

            const to_node = refs[i];
            const to = nodesMap[to_node];

            const bearing = turf.bearing(from, to);

            const length = turf.distance(from, to, { units: "miles" });

            edge_insert_stmt.run(id, i - 1, refs[i - 1], refs[i], bearing, length, tags.highway, 0);
          }
        }

        if (needsReverse) {

          refs.reverse();

          for (let i = 1; i < refs.length; ++i) {

            const from_node = refs[i - 1];
            const from = nodesMap[from_node];

            const to_node = refs[i];
            const to = nodesMap[to_node];

            const bearing = turf.bearing(from, to);

            const length = turf.distance(from, to, { units: "miles" });

            const isReversed = tags.oneway === "-1" ? 0 : 1;
            
            edge_insert_stmt.run(id, i - 1, refs[i - 1], refs[i], bearing, length, tags.highway, isReversed);
          }
        }

        if (++numWays >= logInfoAt) {
          theConflationator.logInfo("LOADED", d3intFormat(numWays), "WAYS");
          logInfoAt += incAmt;
        }
        next();
      }
    });
  }
  async loadWays(client, OSM_ways_table, edge_insert_stmt) {
    const copyToStream = client.query(
      pgCopyStreams.to(`
        COPY (
          SELECT osm_id, ARRAY_TO_JSON(refs), tags
          FROM ${ OSM_ways_table }
        )
        TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
      `)
    );
    this.logInfo("LOADING WAYS");
    await new Promise((resolve, reject) => {
      pipeline(
        copyToStream,
        split2(),
        this.getWayTransform(edge_insert_stmt, getNewNodeId, node_insert_stmt),
        error => {
          if (error) {
            reject(error);
          }
          else {
            resolve();
          }
        }
      )
    })
  }

  getTmcTransform(tmc_insert_stmt) {
    const theConflationator = this;
    const incAmt = 10000;
    let logInfoAt = incAmt;
    let numTMCs = 0;

    return new Transform({
      transform(chunk, encoding, next) {
        const [[tmc, f_system, geojson]] = d3dsvFormatter.parseRows(chunk.toString());

        const mainGeometry = JSON.parse(geojson);

        const MUST_BREAK_AT = 45.0;
        const BEARING_LIMIT = 30.0;
        const BEARING_LENGTH = 0.05;

        const MAX_SEGMENT_MILES = 0.25;
        const MIN_SEGMENT_LENGTH = 0.1;
        // const LENGTH_FUDGE = 0.025;

// HANDLE EACH LINESTRING OF THE MULTILINESTRINGS SEPARATELY
        for (let lsIndex = 0; lsIndex < mainGeometry.coordinates.length; ++lsIndex) {

          const coordinates = [...mainGeometry.coordinates[lsIndex]];

          const geometry = {
            type: "LineString",
            coordinates
          }

          const totalGeometryMiles = turf.length(geometry, { units: "miles" });

          let prevBearing = null;
          let length = 0;

          const linestringSlices = [0];

// TRAVEL ALONG THE TMC AND BREAK IT AT LARGE ANGLES,
// MAINTAINING ORIGINAL POINTS
          for (let i = 1; i < coordinates.length; ++i) {
            const from = coordinates[i - 1];
            const to = coordinates[i];
            const bearing = turf.bearing(from, to);
            length += turf.distance(from, to, { units: "miles" });
            if (i === 1) {
              prevBearing = bearing;
            }
            else {
              if ((Math.abs(prevBearing - bearing) >= MUST_BREAK_AT) ||
                  ((Math.abs(prevBearing - bearing) >= BEARING_LIMIT) &&
                    (length >= BEARING_LENGTH))) {
                linestringSlices.push(i, i - 1);
                prevBearing = bearing;
                length = 0;
              }
            }
          }
          linestringSlices.push(coordinates.length);

          const linestringSegments = [];
          for (let i = 0; i < linestringSlices.length; i += 2) {
            const s1 = linestringSlices[i];
            const s2 = linestringSlices[i + 1];
            linestringSegments.push(coordinates.slice(s1, s2));
          }

          let tmcIndex = 0;

          for (const lsSegment of linestringSegments) {
            const geometry = {
              type: "LineString",
              coordinates: [...lsSegment]
            };
            const segmentMiles = turf.length(geometry, { units: "miles" });

// LOOK FOR LARGE TMC CHUNKS AND BREAK THEM INTO SMALLER CHUNKS,
// MAINTAINING ORIGINAL POINTS
            if (segmentMiles >= MAX_SEGMENT_MILES) {
              const numChunks = Math.ceil(segmentMiles) * 2;
              const idealMiles = segmentMiles / numChunks;

              let totalMiles = 0.0;
              let currentMiles = 0.0;

              const newSlices = [0];

              for (let i = 1; i < lsSegment.length - 1; ++i) {
                const from = lsSegment[i - 1];
                const to = lsSegment[i];
                const dist = turf.distance(from, to, { units: "miles" });
                totalMiles += dist;
                currentMiles += dist;
                const remainingMiles = segmentMiles - totalMiles;
                if ((remainingMiles >= MIN_SEGMENT_LENGTH) &&
                    (currentMiles >= idealMiles)) {
                  newSlices.push(i + 1, i);
                  currentMiles = 0.0;
                }
              }
              newSlices.push(lsSegment.length);

              const newSegments = [];
              for (let i = 0; i < newSlices.length; i += 2) {
                const s1 = newSlices[i];
                const s2 = newSlices[i + 1];
                newSegments.push(lsSegment.slice(s1, s2));
              }

              for (const segment of newSegments) {
                const geometry = {
                  type: "LineString",
                  coordinates: [...segment]
                };
                const segmentMiles = turf.length(geometry, { units: "miles" });

// SAVING LINESTRING INDICES AND TMC INDICES ALLOWS FOR RECREATION OF MULTILINESTRINGS
                tmc_insert_stmt.run(tmc, lsIndex, tmcIndex, segmentMiles, f_system || 6, JSON.stringify(geometry));
                ++tmcIndex;
              }
            }
            else {
              tmc_insert_stmt.run(tmc, lsIndex, tmcIndex, segmentMiles, f_system || 6, JSON.stringify(geometry));
              ++tmcIndex;
            }
          }
        }
        if (++numTMCs >= logInfoAt) {
          theConflationator.logInfo("LOADED", d3intFormat(numTMCs), "TMCs");
          logInfoAt += incAmt;
        }
        next();
      }
    });
  }
  async loadTmcs(client, NPMRDS_table, tmc_insert_stmt) {
    const copyToStream = client.query(
      pgCopyStreams.to(`
        COPY (
          SELECT
            tmc,
            f_system,
            ST_AsGeoJSON(wkb_geometry)
          FROM ${ NPMRDS_table }
          ORDER BY TMC
        )
        TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
      `)
    );
    this.logInfo("LOADING TMCs");
    await new Promise((resolve, reject) => {
      pipeline(
        copyToStream,
        split2(),
        this.getTmcTransform(tmc_insert_stmt),
        error => {
          if (error) {
            reject(error);
          }
          else {
            resolve();
          }
        }
      )
    })
  }
  findIntersections() {
    const sql = `
      WITH
        edges_cte AS (
          SELECT
              from_node AS node_id,
              way_id,
              reversed
            FROM edges

          UNION

          SELECT
              to_node AS node_id,
              way_id,
              reversed
            FROM edges
        ),
        way_counts AS (
          SELECT node_id, COUNT(DISTINCT way_id) AS num_ways
            FROM edges_cte
            GROUP BY 1
        ),
        reversed_nodes AS (
          SELECT DISTINCT node_id
            FROM edges_cte
            WHERE reversed = 1
        )
      INSERT INTO intersections(node_id)
        SELECT DISTINCT node_id
          FROM way_counts
            JOIN reversed_nodes
              USING(node_id)
          WHERE num_ways > 2;
    `;
    this.logInfo("FINDING INTERSECTIONS");
    this.db.run(sql);
  }
}