import { pipeline, Transform } from "node:stream";
import { join, dirname, basename } from "node:path";
import { fileURLToPath } from 'node:url';
import {
  existsSync,
  readdirSync,
  copyFileSync,
  rmSync,
  mkdirSync,
  createWriteStream
} from "node:fs"

import { dsvFormat as d3dsvFormat } from "d3-dsv"
import { format as d3format } from "d3-format"
import { quadtree as d3quadtree } from "d3-quadtree"
import { group as d3group, rollups as d3rollups } from "d3-array"
import { timeFormat as d3timeFormat } from "d3-time-format"

import split2 from "split2"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import createGraph from "ngraph.graph"

import SQLite3DB from "./BetterSQLite3DB.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const ACTIVE_DB_DIRECTORY = join(__dirname, "sqlite");
const ACTIVE_DB_PATH = join(ACTIVE_DB_DIRECTORY, "active_db.sqlite");

const d3intFormat = d3format(",d");
const d3dsvFormatter = d3dsvFormat("|");

export default class MyGraph {
  constructor(config) {
    this.config = { ...config };
    this.checkpoint = null

    this.db = null;

    this.quadtree = d3quadtree().x(d => d.lon).y(d => d.lat);

    this.graph = createGraph();

    const formatter = d3timeFormat("%Y-%m-%d-T%H:%M:%S");
    const logFile = `log-${ formatter(new Date()) }`;
    this.writer = createWriteStream(join(__dirname, "logs", logFile));
  }
  async finalize() {
    await this.db.close();
    await this.writer.close();
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
    this.writer.write(string);
    this.writer.write("\n");
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
    rmSync(ACTIVE_DB_DIRECTORY, { force: true, recursive: true });
  }
  prepSqliteDirectory() {
    this.removeSqliteDirectory();
    mkdirSync(ACTIVE_DB_DIRECTORY);
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
    ] = this.initializeSQLiteDBforCheckpointZero();

    await this.loadNodes(client, OSM_nodes_table, node_insert_stmt);
    await this.loadWays(client, OSM_ways_table, edge_insert_stmt);
    await this.loadTmcs(client, NPMRDS_table, tmc_insert_stmt);

    await client.end();

    this.findIntersections();

    this.reportStatsForCheckpointZero();

    await this.saveCheckpoint("checkpoint-0");

    return this.runCheckpointOne();
  }
  initializeSQLiteDBforCheckpointZero() {
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
        highway TEXT,
        reversed INT NOT NULL,
        PRIMARY KEY(way_id, from_node, to_node),
        FOREIGN KEY(from_node) REFERENCES nodes(node_id) ON DELETE CASCADE,
        FOREIGN KEY(to_node) REFERENCES nodes(node_id) ON DELETE CASCADE
      );
    `);
    const edge_insert_stmt = this.db.prepare("INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, highway, reversed) VALUES(?, ?, ?, ?, ?, ?);");
    
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

  populateGraph() {
    this.logInfo("POPULATING GRAPH WITH EDGES");
    const iterator = this.db.prepare("SELECT * FROM edges;").iterate();
    for (const edge of iterator) {
      const { from_node, to_node } = edge;
      this.graph.addLink(from_node, to_node, edge);
    }
    this.logInfo("LOADED", d3intFormat(this.graph.getNodeCount()), "NODES INTO GRAPH");
    this.logInfo("LOADED", d3intFormat(this.graph.getEdgeCount()), "EDGES INTO GRAPH");
  }
  async runCheckpointOne() {
    this.logInfo("RUNNING CHECKPOINT ONE");

    const [
      newNodeInsertStmt,
      newEdgeInsertStmt,
      oldEdgeInsertStmt,
      nodeInsertStmt,
      edgeInsertStmt,
      edgeDeleteStmt,
      selectNodeStmt
    ] = await this.initializeSQLiteDBforCheckpointOne();

    const maxNodeIdSql = `
      SELECT MAX(node_id) AS max_node_id
        FROM nodes;
    `;
    const { max_node_id } = this.db.get(maxNodeIdSql);
    let node_id = +max_node_id;
    const getNewNodeId = () => {
      return ++node_id;
    }

    const intersectionEdgesSql = `
      SELECT
          way_id,
          pos,
          from_node,
          to_node,
          highway,
          reversed,
          'incoming' AS dir
        FROM edges
          JOIN intersections
            ON edges.to_node = intersections.node_id

      UNION

      SELECT
          way_id,
          pos,
          from_node,
          to_node,
          highway,
          reversed,
          'outgoing' AS dir
        FROM edges
          JOIN intersections
            ON edges.from_node = intersections.node_id
    `;
    this.logInfo("QUERYING INTERSECTION EDGES");
    const intersectionEdges = this.db.all(intersectionEdgesSql);
    const grouped = d3group(intersectionEdges, e => e.dir, e => e.dir === "incoming" ? e.to_node : e.from_node);

    const incomingEdgesMap = grouped.get("incoming");
    const outgoingEdgesMap = grouped.get("outgoing");

    const intersectionNodesSql = `
      SELECT DISTINCT nodes.node_id, lon, lat
        FROM nodes
          JOIN intersections
            USING(node_id);
    `;
    this.logInfo("QUERYING INTERSECTION NODES");
    const intersectionNodes = this.db.all(intersectionNodesSql);

    const nodeIncAmt = 10000;
    let logInfoAt = nodeIncAmt;
    let numIntersections = 0;

    this.logInfo("PROCESSING INTERSECTIONS");
    for (const iNode of intersectionNodes) {
      const { node_id: base_node_id } = iNode;

      const incomingEdges = incomingEdgesMap.get(base_node_id);
      const outgoingEdges = outgoingEdgesMap.get(base_node_id);

      if (incomingEdges && outgoingEdges) {

// CREATE NEW INCOMING NODES AND EDGES
        const newIncomingEdges = incomingEdges.map(inEdge => {

          const newNodeId = getNewNodeId();

          const newNode = {
            ...iNode,
            node_id: newNodeId,
            base_node_id
          };
          newNodeInsertStmt.run(newNode);
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...inEdge,
            to_node: newNodeId
          };
          newEdgeInsertStmt.run(newEdge);
          edgeInsertStmt.run(newEdge);

          oldEdgeInsertStmt.run(inEdge);
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
          newNodeInsertStmt.run(newNode);
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...outEdge,
            from_node: newNodeId
          };
          newEdgeInsertStmt.run(newEdge);
          edgeInsertStmt.run(newEdge);

          oldEdgeInsertStmt.run(outEdge);
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
                to_node: outEdge.from_node
              }
              newEdgeInsertStmt.run(newEdge);
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

    const getReversedEdgesSql = `
      SELECT
          way_id,
          pos,
          from_node,
          to_node,
          highway,
          reversed
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

      const newNodes = [];

      for (let i = 0; i < way.length; ++i) {

        const edge = way[i];

        if (i === (way.length - 1)) {
          const newEdge = {
            ...edge,
            from_node: newNodes[newNodes.length -1].node_id
          };

          newEdgeInsertStmt.run(newEdge);
          edgeInsertStmt.run(newEdge);

          oldEdgeInsertStmt.run(edge);
          edgeDeleteStmt.run(edge);
        }
        else if (i === 0) {
          const toNode = selectNodeStmt.get(edge.to_node);

          const newNodeId = getNewNodeId();

          const newNode = {
            ...toNode,
            node_id: newNodeId,
            base_node_id: toNode.node_id
          };
          newNodes.push(newNode);

          newNodeInsertStmt.run(newNode);
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...edge,
            to_node: newNodeId
          };

          newEdgeInsertStmt.run(newEdge);
          edgeInsertStmt.run(newEdge);

          oldEdgeInsertStmt.run(edge);
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

          newNodeInsertStmt.run(newNode);
          nodeInsertStmt.run(newNode);

          const newEdge = {
            ...edge,
            from_node: newNodes[newNodes.length -1].node_id,
            to_node: newNodeId
          };
          newNodes.push(newNode);

          newEdgeInsertStmt.run(newEdge);
          edgeInsertStmt.run(newEdge);

          oldEdgeInsertStmt.run(edge);
          edgeDeleteStmt.run(edge);
        }
      }

      if (++numWays >= logInfoAt) {
        this.logInfo("PROCESSED", d3intFormat(numWays), "REVERSED WAYS");
        logInfoAt += wayIncAmt;
      }
    }

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
  initializeSQLiteDBforCheckpointOne() {
    this.logInfo("INITIALIZING SQLITE DB FOR CHECKPOINT ONE");

    this.db.run(`
      CREATE TABLE new_nodes(
        node_id INT PRIMARY KEY,
        lon DOUBLE PRECISION,
        lat DOUBLE PRECISION,
        base_node_id INT
      );
    `);
    const newNodeInsertStmt = this.db.prepare("INSERT INTO new_nodes(node_id, lon, lat, base_node_id) VALUES($node_id, $lon, $lat, $base_node_id)");

    this.db.run(`
      CREATE TABLE new_edges(
        way_id INT NOT NULL,
        pos INT NOT NULL,
        from_node INT NOT NULL,
        to_node INT NOT NULL,
        highway TEXT,
        reversed INT NOT NULL,
        PRIMARY KEY(way_id, from_node, to_node)
      );
    `);
    const newEdgeInsertStmt = this.db.prepare("INSERT INTO new_edges(way_id, pos, from_node, to_node, highway, reversed) VALUES($way_id, $pos, $from_node, $to_node, $highway, $reversed)");

    this.db.run(`
      CREATE TABLE old_edges(
        way_id INT,
        from_node INT,
        to_node INT,
        PRIMARY KEY(way_id, from_node, to_node)
      )
    `);
    const oldEdgeInsertStmt = this.db.prepare("INSERT OR REPLACE INTO old_edges(way_id, from_node, to_node) VALUES($way_id, $from_node, $to_node)");

    const nodeInsertStmt = this.db.prepare("INSERT INTO nodes(node_id, lon, lat, base_node_id) VALUES($node_id, $lon, $lat, $base_node_id)");
    const edgeInsertStmt = this.db.prepare("INSERT INTO edges(way_id, pos, from_node, to_node, highway, reversed) VALUES($way_id, $pos, $from_node, $to_node, $highway, $reversed)");
    const edgeDeleteStmt = this.db.prepare("DELETE FROM edges WHERE (way_id, from_node, to_node) = ($way_id, $from_node, $to_node);");
    const selectNodeStmt = this.db.prepare("SELECT node_id, lon, lat, base_node_id FROM nodes WHERE node_id = ?;");

    return [
      newNodeInsertStmt,
      newEdgeInsertStmt,
      oldEdgeInsertStmt,
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
    const incAmt = 125000;
    let logInfoAt = incAmt;
    let numWays = 0;
    return new Transform({
      transform(chunk, encoding, next) {
        const [parsedRow] = d3dsvFormatter.parseRows(chunk.toString());
        const [id, refs, tags] = parsedRow.map(JSON.parse);
        for (let i = 0; i < refs.length - 1; ++i) {
          edge_insert_stmt.run(id, i, refs[i], refs[i + 1], tags.highway, 0);
        }
        if (tags.oneway !== "yes") {
          let pos = 0;
          for (let i = refs.length - 1; i > 1; --i) {
            edge_insert_stmt.run(id, pos++, refs[i], refs[i - 1], tags.highway, 1);
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
        const [[tmc, miles, f_system, geojson]] = d3dsvFormatter.parseRows(chunk.toString());
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
            miles,
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
}
