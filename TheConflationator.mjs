import { pipeline, Transform } from "node:stream";
import { join, dirname, basename } from "node:path";
import { fileURLToPath } from 'node:url';
import {
  existsSync,
  readdirSync,
  copyFileSync,
  rmSync,
  mkdirSync
} from "node:fs"

import { dsvFormat as d3dsvFormat } from "d3-dsv"
import { format as d3format } from "d3-format"
import { quadtree as d3quadtree } from "d3-quadtree"
import { group as d3group, rollups as d3rollups } from "d3-array"
import { scaleLinear as d3scaleLinear } from "d3-scale"
// import { timeFormat as d3timeFormat } from "d3-time-format"

import * as turf from "@turf/turf";

import split2 from "split2"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import createGraph from "ngraph.graph"
import PathFinders from "ngraph.path"

import SQLite3DB from "./BetterSQLite3DB.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const WORKING_DIRECTORY = join(__dirname, "sqlite");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

const d3intFormat = d3format(",d");
const d3dsvFormatter = d3dsvFormat("|");

const COERCES_TO_NUMBERS = ["", null, true, false, []]
const strictNaN = value => {
  return COERCES_TO_NUMBERS.includes(value) ?  true : isNaN(value);
}

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
    const checkpoints = readdirSync("./checkpoints").sort((a, b) => b.localeCompare(a));
    if (checkpoints.length) {
      const sqliteFile = checkpoints[0];
      this.checkpoint = basename(sqliteFile, ".sqlite");
      this.db = this.loadFromDisk(join("./checkpoints", sqliteFile));
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
    const filepath = join("./checkpoints", `${ checkpoint }.sqlite`);
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
      case "checkpoint-2":
        return (async () => { this.logInfo("NO CHECKPOINT 3"); })();
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

    const client = new pgStuff.Client(npmrds2);
    await client.connect();

    const OSM_ways_table = await this.getDataTable(client, OSM_view_id);
    const OSM_nodes_table = `${ OSM_ways_table }_nodes`;

    const NPMRDS_table = await this.getDataTable(client, NPMRDS_view_id);

    const [
      node_insert_stmt,
      edge_insert_stmt,
      tmc_insert_stmt
    ] = this.initializeCheckpointZero();

    await this.loadNodes(client, OSM_nodes_table, node_insert_stmt);
    await this.loadWays(client, OSM_ways_table, edge_insert_stmt);
    await this.loadTmcs(client, NPMRDS_table, tmc_insert_stmt);

    await client.end();

    this.findIntersections();
    // this.findConnections();

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
        pos INT NOT NULL,
        from_node INT NOT NULL,
        to_node INT NOT NULL,
        bearing DOUBLE PRECISION,
        length DOUBLE PRECISION,
        highway TEXT,
        reversed INT NOT NULL,
        PRIMARY KEY(way_id, from_node, to_node),
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
        tmc TEXT PRIMARY KEY,
        miles DOUBLE PRECISION,
        f_system INT,
        geojson TEXT
      );
    `);
    const tmc_insert_stmt = this.db.prepare("INSERT INTO tmcs(tmc, miles, f_system, geojson) VALUES(?, ?, ?, ?);");

    this.db.run(`
      CREATE TABLE IF NOT EXISTS intersections(
        node_id INT PRIMARY KEY
      );
    `);

    // this.db.run(`
    //   CREATE TABLE IF NOT EXISTS connections(
    //     node_id INT PRIMARY KEY
    //   );
    // `);

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

    // const connectionCountSql = `
    //   SELECT COUNT(1) AS count
    //   FROM connections
    // `;
    // const { count: connections } = this.db.get(connectionCountSql);
    // this.logInfo("LOADED", d3intFormat(connections), "CONNECTIONS INTO SQLITE DB");
  }

  async runCheckpointOne() {
    this.logInfo("RUNNING CHECKPOINT ONE");

    const [
      // newNodeInsertStmt,
      // newEdgeInsertStmt,
      // oldEdgeInsertStmt,
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

// HANDLE INTERSECTIONS
// (INTERSECTIONS ARE NODES WHERE MORE THAN 2 WAY IDs MEET)
    const intersectionNodesSql = `
      SELECT DISTINCT nodes.node_id, lon, lat
        FROM nodes
          JOIN intersections
            USING(node_id);
    `;
    this.logInfo("QUERYING INTERSECTION NODES");
    const intersectionNodes = this.db.all(intersectionNodesSql);
    this.logInfo("RECEIVED", d3intFormat(intersectionNodes.length), "INTERSECTION NODES");

    const intersectionEdgesSql = `
     SELECT edges.*,
          'incoming' AS dir
        FROM edges
          WHERE edges.to_node = $base_node_id

      UNION

      SELECT edges.*,
          'outgoing' AS dir
        FROM edges
          WHERE edges.from_node = $base_node_id
    `;
    const intersectionEdgesStmt = this.db.prepare(intersectionEdgesSql);

    const nodeIncAmt = 10000;
    let logInfoAt = nodeIncAmt;
    let numIntersections = 0;

    this.logInfo("PROCESSING INTERSECTIONS");
    for (const iNode of intersectionNodes) {
      const { node_id: base_node_id } = iNode;

      const intersectionEdges = intersectionEdgesStmt.all({ base_node_id });
      const groupedIntersections = d3group(intersectionEdges, e => e.dir, e => e.dir === "incoming" ? e.to_node : e.from_node);

      const incomingIntersectionEdgesMap = groupedIntersections.get("incoming");
      const outgoingIntersectionEdgesMap = groupedIntersections.get("outgoing");

      if (!(incomingIntersectionEdgesMap && outgoingIntersectionEdgesMap)) continue;

      const incomingEdges = incomingIntersectionEdgesMap.get(base_node_id);
      const outgoingEdges = outgoingIntersectionEdgesMap.get(base_node_id);

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
// THEY MAKE NO SENSE SO I IGNORE THEM.
      if (incomingEdges && outgoingEdges) {

// CREATE NEW INCOMING NODES AND EDGES
        const newIncomingEdges = incomingEdges.map((inEdge, i) => {
          if (i === 0) {
            return inEdge;
          }

          const newNodeId = getNewNodeId();

          const newNode = {
            ...iNode,
            node_id: newNodeId,
            base_node_id
          };
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...inEdge,
            to_node: newNodeId
          };
          edgeInsertStmt.run(newEdge);
          edgeDeleteStmt.run(inEdge);

          return newEdge;
        });

// CREATE NEW OUTGOING NODES AND EDGES
        const newOutgoingEdges = outgoingEdges.map(outEdge => {

          const newNodeId = getNewNodeId();

          const newNode = {
            ...iNode,
            node_id: newNodeId,
            base_node_id
          };
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...outEdge,
            from_node: newNodeId
          };
          edgeInsertStmt.run(newEdge);
          edgeDeleteStmt.run(outEdge);

          return newEdge;
        });

// CONNECT NEW INCOMING EDGES TO NEW OUTGOING EDGES
// (EXCEPT FOR IT'S OWN REVERSED OUTGOING EDGE)
        for (const inEdge of newIncomingEdges) {
          for (const outEdge of newOutgoingEdges) {
            if (inEdge.way_id !== outEdge.way_id) {
              const newEdge = {
                ...inEdge,
                from_node: inEdge.to_node,
                to_node: outEdge.from_node,
                length: 0
              }
              // newEdgeInsertStmt.run(newEdge);
              edgeInsertStmt.run(newEdge);
            }
          }
        }

        if (++numIntersections >= logInfoAt) {
          this.logInfo("PROCESSED", d3intFormat(numIntersections), "INTERSECTIONS");
          logInfoAt += nodeIncAmt;
        }
      }
    }
    this.logInfo("PROCESSED", d3intFormat(numIntersections),"INTERSECTIONS NODES");

// HANDLE CONNECTIONS
// (CONNECTIONS ARE NODES WHERE EXACTLY 2 DIFFERENT WAY IDs MEET)
    // const connectionNodesSql = `
    //   SELECT DISTINCT nodes.node_id, lon, lat
    //     FROM nodes
    //       JOIN connections
    //         USING(node_id);
    // `;
    // this.logInfo("QUERYING CONNECTION NODES");
    // const connectionNodes = this.db.all(connectionNodesSql);
    // this.logInfo("RECEIVED", d3intFormat(connectionNodes.length), "CONNECTION NODES");

    // const connectionEdgesSql = `
    //   SELECT edges.*,
    //       'incoming' AS dir
    //     FROM edges
    //       WHERE edges.to_node = $node_id

    //   UNION

    //   SELECT edges.*,
    //       'outgoing' AS dir
    //     FROM edges
    //       WHERE edges.from_node = $node_id
    // `;
    // const connectionEdgesStmt = this.db.prepare(connectionEdgesSql);

    // // const nodeIncAmt = 10000;
    // logInfoAt = nodeIncAmt;
    // let numConnections = 0;

    // this.logInfo("PROCESSING CONNECTION NODES");
    // for (const cNode of connectionNodes) {
    //   const { node_id } = cNode;

    //   const connectionEdges = connectionEdgesStmt.all({ node_id });
    //   const groupedConnections = d3group(connectionEdges, e => e.dir, e => e.dir === "incoming" ? e.to_node : e.from_node);

    //   const incomingConnectionEdgesMap = groupedConnections.get("incoming");
    //   const outgoingConnectionEdgesMap = groupedConnections.get("outgoing");

    //   if (!(incomingConnectionEdgesMap && outgoingConnectionEdgesMap)) continue;

    //   const incomingEdges = incomingConnectionEdgesMap.get(node_id);
    //   const outgoingEdges = outgoingConnectionEdgesMap.get(node_id);

    //   if (incomingEdges && (incomingEdges.length === 2) &&
    //       outgoingEdges && (outgoingEdges.length === 2)) {

    //     const [inEdge1, inEdge2] = incomingEdges;
    //     const [outEdge1, outEdge2] = outgoingEdges;

    //     if ((inEdge1.to_node === outEdge1.from_node) &&
    //         (inEdge2.to_node === outEdge2.from_node)) {

    //       const newNodeId = getNewNodeId();

    //       const newNode = {
    //         ...cNode,
    //         node_id: newNodeId,
    //         base_node_id: node_id
    //       };
    //       nodeInsertStmt.run(newNode);

    //       const newInEdge = {
    //         ...inEdge2,
    //         to_node: newNodeId
    //       };
    //       edgeInsertStmt.run(newInEdge);
    //       edgeDeleteStmt.run(inEdge2);

    //       const newOutEdge = {
    //         ...outEdge2,
    //         from_node: newNodeId
    //       };
    //       edgeInsertStmt.run(newOutEdge);
    //       edgeDeleteStmt.run(outEdge2);
          
    //       if (++numConnections >= logInfoAt) {
    //         this.logInfo("PROCESSED", d3intFormat(numConnections), "CONNECTIONS");
    //         logInfoAt += nodeIncAmt;
    //       }
    //     }
    //     else if ((inEdge1.to_node === outEdge2.from_node) &&
    //               (inEdge2.to_node === outEdge1.from_node)) {

    //       const newNodeId = getNewNodeId();

    //       const newNode = {
    //         ...cNode,
    //         node_id: newNodeId,
    //         base_node_id: node_id
    //       };
    //       nodeInsertStmt.run(newNode);

    //       const newInEdge = {
    //         ...inEdge2,
    //         to_node: newNodeId
    //       };
    //       edgeInsertStmt.run(newInEdge);
    //       edgeDeleteStmt.run(inEdge2);

    //       const newOutEdge = {
    //         ...outEdge1,
    //         from_node: newNodeId
    //       };
    //       edgeInsertStmt.run(newOutEdge);
    //       edgeDeleteStmt.run(outEdge1);

    //       if (++numConnections >= logInfoAt) {
    //         this.logInfo("PROCESSED", d3intFormat(numConnections), "CONNECTIONS");
    //         logInfoAt += nodeIncAmt;
    //       }
    //     }
    //   }
    // }
    // this.logInfo("PROCESSED", d3intFormat(numConnections),"CONNECTION NODES");

// HANDLE REVERSED WAYS
    const getReversedEdgesSql = `
      SELECT *
        FROM edges
        WHERE reversed = 1
    `;
    this.logInfo("QUERYING FOR REVERSED EDGES");
    const reversedEdges = this.db.all(getReversedEdgesSql);
    this.logInfo("RECEIVED", d3intFormat(reversedEdges.length), "REVERSED EDGES");

    const sorter = group => group.sort((a, b) => +a.pos - +b.pos);
    const rollups = d3rollups(reversedEdges, sorter, e => e.way_id).filter(([way_id, way]) => way.length > 1);
    this.logInfo("PROCESSING", d3intFormat(rollups.length), "REVERSED WAYS");

    const wayIncAmt = 50000;
    logInfoAt = wayIncAmt;
    let numWays = 0;

    for (const [way_id, way] of rollups) {

      if (way.length < 2) {
        continue;
      }

      const newNodeIds = [];

      for (let i = 0; i < way.length; ++i) {

        const edge = way[i];

        if (i === (way.length - 1)) {
          const newEdge = {
            ...edge,
            from_node: newNodeIds.at(-1)
          };
          edgeInsertStmt.run(newEdge);
          edgeDeleteStmt.run(edge);
        }
        else if (i === 0) {
          const toNode = selectNodeStmt.get(edge.to_node);

          const newNodeId = getNewNodeId();
          newNodeIds.push(newNodeId);

          const newNode = {
            ...toNode,
            node_id: newNodeId,
            base_node_id: toNode.node_id
          };
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...edge,
            to_node: newNodeId
          };
          edgeInsertStmt.run(newEdge);
          edgeDeleteStmt.run(edge);
        }
        else {
          const toNode = selectNodeStmt.get(edge.to_node);

          const newNodeId = getNewNodeId();

          const newNode = {
            ...toNode,
            node_id: newNodeId,
            base_node_id: toNode.node_id
          };
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...edge,
            from_node: newNodeIds.at(-1),
            to_node: newNodeId
          };
          newNodeIds.push(newNodeId);
          edgeInsertStmt.run(newEdge);
          edgeDeleteStmt.run(edge);
        }
      }

      if (++numWays >= logInfoAt) {
        this.logInfo("PROCESSED", d3intFormat(numWays), "REVERSED WAYS");
        logInfoAt += wayIncAmt;
      }
    }
    this.logInfo("PROCESSED", d3intFormat(numWays),"REVERSED WAYS");

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

    // this.db.run(`
    //   CREATE TABLE new_nodes(
    //     node_id INT PRIMARY KEY,
    //     lon DOUBLE PRECISION,
    //     lat DOUBLE PRECISION,
    //     base_node_id INT
    //   );
    // `);
    // const newNodeInsertStmt = this.db.prepare("INSERT INTO new_nodes(node_id, lon, lat, base_node_id) VALUES($node_id, $lon, $lat, $base_node_id)");

    // this.db.run(`
    //   CREATE TABLE new_edges(
    //     way_id INT NOT NULL,
    //     pos INT NOT NULL,
    //     from_node INT NOT NULL,
    //     to_node INT NOT NULL,
    //     highway TEXT,
    //     bearing DOUBLE PRECISION,
    //     reversed INT NOT NULL,
    //     length DOUBLE PRECISION NOT NULL DEFAULT 0,
    //     PRIMARY KEY(way_id, from_node, to_node)
    //   );
    // `);
    // const newEdgeInsertStmt = this.db.prepare("INSERT INTO new_edges(way_id, pos, from_node, to_node, highway, reversed, length, bearing) VALUES($way_id, $pos, $from_node, $to_node, $highway, $reversed, $length, $bearing)");

    // this.db.run(`
    //   CREATE TABLE old_edges(
    //     way_id INT,
    //     from_node INT,
    //     to_node INT,
    //     PRIMARY KEY(way_id, from_node, to_node)
    //   )
    // `);
    // const oldEdgeInsertStmt = this.db.prepare("INSERT OR REPLACE INTO old_edges(way_id, from_node, to_node) VALUES($way_id, $from_node, $to_node)");

    const nodeInsertStmt = this.db.prepare("INSERT INTO nodes(node_id, lon, lat, base_node_id) VALUES($node_id, $lon, $lat, $base_node_id)");
    const edgeInsertStmt = this.db.prepare("INSERT INTO edges(way_id, pos, from_node, to_node, highway, reversed, length, bearing) VALUES($way_id, $pos, $from_node, $to_node, $highway, $reversed, $length, $bearing)");
    const edgeDeleteStmt = this.db.prepare("DELETE FROM edges WHERE (way_id, from_node, to_node) = ($way_id, $from_node, $to_node);");
    const selectNodeStmt = this.db.prepare("SELECT node_id, lon, lat, base_node_id FROM nodes WHERE node_id = ?;");

    return [
      // newNodeInsertStmt,
      // newEdgeInsertStmt,
      // oldEdgeInsertStmt,
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
      pathFinder,
      insertResultStmt
    ] = await this.initializeCheckpointTwo();

    const DISTANCE_THRESHOLD = Math.sqrt(2 * Math.pow(500 / 5280, 2));
    const NUM_EDGES_TO_KEEP = 3;
    const NUM_RESULTS_TO_KEEP = 3;

    const bearingScale = d3scaleLinear([0, 180], [60, 0]);

    const HIGHWAY_TO_F_SYSTEM_MAP = {
      motorway: 1,
      motorway_link: 1,

      trunk: 2,
      trunk_link: 2,

      primary: 3,
      primary_link: 3,

      secondary: 4,
      secondary_link: 4,

      tertiary: 5,
      tertiary_link: 5,

      unclassified: 6,

      residential: 7,
      living_street: 7
    }

    const bearing = edge => {
      const b = turf.bearing(...edge);
      return b < 0 ? b + 360 : b;
    }

    const rankEdgeCandidate = (candidateEdge, tmcEdge) => {
      const candidateEdgeBearing = candidateEdge.bearing;
      const tmcEdgeBearing = bearing(tmcEdge.edge);
      const bearingDiff = Math.abs(tmcEdgeBearing - candidateEdgeBearing);
      const bearingValue = bearingScale(bearingDiff);

      const candidateEdgeFsystem = HIGHWAY_TO_F_SYSTEM_MAP[candidateEdge.highway];
      const fSystemValue = 60 - Math.abs(+tmcEdge.f_system - candidateEdgeFsystem) * 10;

      return bearingValue + fSystemValue;
    }

    const findAllWithin = (tmcEdge, exclude = []) => {

      const [[lon1, lat1], [lon2, lat2]] = tmcEdge.edge;

      const [xmin, ymax] = turf.destination([Math.min(lon1, lon2), Math.max(lat1, lat2)], DISTANCE_THRESHOLD, -45).geometry.coordinates;
      const [xmax, ymin] = turf.destination([Math.max(lon1, lon2), Math.min(lat1, lat2)], DISTANCE_THRESHOLD, 135).geometry.coordinates;

      const edges = new Set();

      quadtree.visit((node, x1, y1, x2, y2) => {
        if (!node.length) {
          do {
            const d = node.data;
            if (d.lon >= xmin && d.lon < xmax && d.lat >= ymin && d.lat < ymax) {
              edges.add(d.edge);

            }
          } while (node = node.next);
        }
        return x1 >= xmax || y1 >= ymax || x2 < xmin || y2 < ymin;
      });

      return [...edges];
    }

    const tmcsSql = `
      SELECT *
        FROM tmcs
          ORDER BY tmc
          LIMIT 1000;
    `;
    const TMCs = this.db.all(tmcsSql)
                        .map(TMC => ({ ...TMC, geometry: JSON.parse(TMC.geojson) }));
    this.logInfo("PROCESSING", d3intFormat(TMCs.length), "TMCs");

    const incAmt = 50;
    let logInfoAt = incAmt;
    let numTMCs = 0;

    for (const TMC of TMCs) {
      const {
        tmc, miles, f_system,
        geometry
      } = TMC;

      const edgePairs = [];

      if (geometry.type === "LineString") {
        const linestring = geometry.coordinates;
        edgePairs.push([
          { tmc, miles, f_system,
            edge: linestring.slice(0, 2)
          },
          { tmc, miles, f_system,
            edge: linestring.slice(-2)
          }
        ]);
      }
      else if (geometry.type === "MultiLineString") {
        for (const linestring of geometry.coordinates) {
          edgePairs.push([
            { tmc, miles, f_system,
              edge: linestring.slice(0, 2)
            },
            { tmc, miles, f_system,
              edge: linestring.slice(-2)
            }
          ]);
        }
      }

      for (const [startEdge, endEdge] of edgePairs) {

        const startCandidateEdges = findAllWithin(startEdge);
        for (const candidateEdge of startCandidateEdges) {
          candidateEdge.rank = rankEdgeCandidate(candidateEdge, startEdge);
        }
        const topStartCandidateEdges = startCandidateEdges.sort((a, b) => b.rank - a.rank)
                                                          .slice(0, NUM_EDGES_TO_KEEP);

        const endCandidateEdges = findAllWithin(endEdge);
        for (const candidateEdge of endCandidateEdges) {
          candidateEdge.rank = rankEdgeCandidate(candidateEdge, endEdge);
        }
        const topEndCandidateEdges = endCandidateEdges.sort((a, b) => b.rank - a.rank)
                                                          .slice(0, NUM_EDGES_TO_KEEP);

        const results = [];

        for (const startCandidateEdge of topStartCandidateEdges) {
          for (const endCandidateEdge of topEndCandidateEdges) {

            const path = pathFinder.find(startCandidateEdge.from_node, endCandidateEdge.to_node)
              .reverse().map(node => {
                const { id, data: { lon, lat } } = node;
                return { node_id: id, lon, lat };
              });

            const calculatedMiles = path.reduce((a, c, i, path) => {
              if (i === 0) return a;

              const fromNode = path[i - 1];
              const from = [fromNode.lon, fromNode.lat];

              const toNode = path[i];
              const to = [toNode.lon, toNode.lat];

              return a + turf.distance(from, to, { units: "miles" });
            }, 0);

            results.push([path, calculatedMiles]);

          }
        }

        results.sort((a, b) => {
          const aMiles = a[1];
          const aDiff = Math.abs(aMiles - miles);

          const bMiles = b[1];
          const bDiff = Math.abs(bMiles - miles);

          return aDiff - bDiff;
        }).slice(0, NUM_RESULTS_TO_KEEP)
          .forEach(([path, calculatedMiles], i) => {
            insertResultStmt.run(tmc, JSON.stringify(path), i + 1, calculatedMiles);
          });
      }

      if (++numTMCs >= logInfoAt) {
        this.logInfo("PROCESSED:", d3intFormat(numTMCs), "TMCs");
        logInfoAt += incAmt;
      }
    }

    await this.reportStatsForCheckpointTwo();

    await this.saveCheckpoint("checkpoint-2");
  }
  async initializeCheckpointTwo() {
    const quadtree = d3quadtree().x(n => n.lon).y(n => n.lat);
    const graph = createGraph();

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
    const edgesSql = `
      SELECT way_id, from_node, to_node, highway, bearing, length
        FROM edges
    `;
    const edgesIterator = this.db.prepare(edgesSql).iterate();
    for (const edge of edgesIterator) {
      const { from_node, to_node } = edge;

      graph.addLink(from_node, to_node, edge);

      const fromNode = graph.getNode(from_node).data;
      const toNode = graph.getNode(to_node).data;

      const qtEdge = {
        ...edge,
        edge: [[fromNode.lon, fromNode.lat], [toNode.lon, toNode.lat]]
      }

      const fromObject = {
        edge: qtEdge,
        lon: fromNode.lon,
        lat: fromNode.lat
      }
      quadtree.add(fromObject);

      const toObject = {
        edge: qtEdge,
        lon: toNode.lon,
        lat: toNode.lat
      }
      quadtree.add(toObject);

      if (++numEdges >= logInfoAt) {
        this.logInfo("LOADED", d3intFormat(numEdges), "EDGES");
        logInfoAt += incAmt;
      }
    }

    const pathFinder = PathFinders.nba(graph, {
      oriented: true,
      distance(fromNode, toNode, link) {
        return link.data.length;
      },
      heuristic(fromNode, toNode) {
        const dlon = toNode.data.lon - fromNode.data.lon;
        const dlat = toNode.data.lat - fromNode.data.lat;
        return Math.sqrt(dlon * dlon + dlat * dlat);
      }
    });

    const createResultsTableSql = `
      CREATE TABLE results(
        tmc TEXT,
        path TEXT,
        rank INT,
        miles DOUBLE PRECISION
      )
    `;
    this.db.run(createResultsTableSql);
    const insertResultSql = `
      INSERT INTO results(tmc, path, rank, miles)
        VALUES (?, ?, ?, ?);
    `;
    const insertResultStmt = this.db.prepare(insertResultSql);

    return [
      quadtree,
      graph,
      pathFinder,
      insertResultStmt
    ];
  }

  async reportStatsForCheckpointTwo() {

  }

  getNodeTransform(node_insert_stmt) {
    const myGraph = this;
    const incAmt = 500000;
    let logInfoAt = incAmt;
    let numNodes = 0;
    return new Transform({
      transform(chunk, encoding, next) {
        const [id, lon, lat] = chunk.toString().split("|").map(Number);
        node_insert_stmt.run(id, lon, lat);
        if (++numNodes >= logInfoAt) {
          myGraph.logInfo("LOADED", d3intFormat(numNodes), "NODES");
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

  getWayTransform(edge_insert_stmt) {
    const myGraph = this;
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

        const nodes = myGraph.db.all(selectNodesSql.replace("__NODE_IDs__", refs))
            .reduce((a, c) => { a[c.node_id] = [c.lon, c.lat]; return a; }, {});

        for (let i = 0; i < refs.length - 1; ++i) {

          const from = nodes[refs[i]];
          const to = nodes[refs[i + 1]];
          let bearing = turf.bearing(from, to);
          if (bearing < 0) bearing += 360;

          const length = turf.distance(from, to, { units: "miles" });

          edge_insert_stmt.run(id, i, refs[i], refs[i + 1], bearing, length, tags.highway, 0);
        }
        if (tags.oneway !== "yes") {

          refs.reverse();

          for (let i = 0; i < refs.length - 1; ++i) {

            const from = nodes[refs[i]];
            const to = nodes[refs[i + 1]];
            let bearing = turf.bearing(from, to);
            if (bearing < 0) bearing += 360;

            const length = turf.distance(from, to, { units: "miles" });
            
            edge_insert_stmt.run(id, i, refs[i], refs[i + 1], bearing, length, tags.highway, 1);
          }
        }
        if (++numWays >= logInfoAt) {
          myGraph.logInfo("LOADED", d3intFormat(numWays), "WAYS");
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
        this.getWayTransform(edge_insert_stmt),
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
    const myGraph = this;
    return new Transform({
      transform(chunk, encoding, next) {
        let [[tmc, f_system, geojson]] = d3dsvFormatter.parseRows(chunk.toString());

        const miles = turf.length(JSON.parse(geojson), { units: "miles" });

        tmc_insert_stmt.run(tmc, miles, f_system, geojson);

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
              way_id
            FROM edges

          UNION

          SELECT
              to_node AS node_id,
              way_id
            FROM edges
        ),
        counts AS (
          SELECT node_id, COUNT(DISTINCT way_id) AS num_ways
            FROM edges_cte
            GROUP BY 1
        )
      INSERT INTO intersections(node_id)
        SELECT DISTINCT node_id
          FROM counts
          WHERE num_ways > 2;
    `;
    this.logInfo("FINDING INTERSECTIONS");
    this.db.run(sql);
  }
  // findConnections() {
  //   const sql = `
  //     WITH
  //       edges_cte AS (
  //         SELECT
  //             from_node AS node_id,
  //             way_id
  //           FROM edges

  //         UNION

  //         SELECT
  //             to_node AS node_id,
  //             way_id
  //           FROM edges
  //       ),
  //       way_counts AS (
  //         SELECT node_id, COUNT(DISTINCT way_id) AS num_ways
  //           FROM edges_cte
  //           GROUP BY 1
  //       ),
  //       edge_counts AS (
  //         SELECT node_id, COUNT(1) AS num_edges
  //           FROM edges_cte
  //           GROUP BY 1
  //       )
  //     INSERT INTO connections(node_id)
  //       SELECT DISTINCT node_id
  //         FROM way_counts
  //           JOIN edge_counts
  //             USING(node_id)
  //         WHERE num_ways = 2
  //         AND num_edges = 4;
  //   `;
  //   this.logInfo("FINDING CONNECTIONS");
  //   this.db.run(sql);
  // }
}
