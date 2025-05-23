import { pipeline, Transform } from "node:stream";
import { join } from "node:path";
import { existsSync, readdirSync } from "node:fs"

import { dsvFormat as d3dsvFormat } from "d3-dsv"
import { format as d3format } from "d3-format"
import { quadtree as d3quadtree } from "d3-quadtree"
import { group as d3group } from "d3-array"
import split2 from "split2"

import pgStuff from "pg";
import pgCopyStreams from "pg-copy-streams";

import createGraph from "ngraph.graph"

import SQLite3DB from "./BetterSQLite3DB.mjs"

export default class MyGraph {
  constructor(config) {
    this.config = { ...config };
    this.checkpoint = "checkpoint-0";

    this.db = null;

    this.quadtree = d3quadtree().x(d => d.lon).y(d => d.lat);

    this.graph = createGraph();
  }
  async finalize() {
    await this.db.close();
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
  loadLatestCheckpoint() {
    const checkpoints = readdirSync("./checkpoints");
    checkpoints.push("checkpoint-0");
    checkpoints.sort((a, b) => {
      const [, anum] = a.split("-");
      const [, bnum] = b.split("-");
      return +bnum - +anum;
    });
    this.checkpoint = checkpoints[0];
    this.db = this.loadFromDisk(join("./checkpoints", this.checkpoint));
    this.logInfo("LOADED CHECKPOINT:", this.checkpoint);
    return this;
  }
  async run() {
    switch (this.checkpoint) {
      case "checkpoint-1":
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
    ] = await this.initializeSQLiteDBforCheckpointZero();

    await this.loadNodes(client, OSM_nodes_table, node_insert_stmt);
    await this.loadWays(client, OSM_ways_table, edge_insert_stmt);
    await this.loadTmcs(client, NPMRDS_table, tmc_insert_stmt);

    await client.end();

    await this.findIntersections();

    await this.reportStats();

    await this.saveCheckpoint("checkpoint-1");

    await this.runCheckpointOne();
  }
  async initializeSQLiteDBforCheckpointZero() {
    this.db.run(`
      CREATE TABLE IF NOT EXISTS nodes(
        node_id INT PRIMARY KEY,
        lon DOUBLE PRECISION,
        lat DOUBLE PRECISION,
        base_node_id INT
      );
    `);
    const node_insert_stmt = await this.db.prepare("INSERT INTO nodes(node_id, lon, lat) VALUES(?, ?, ?);");

    this.db.run(`
      CREATE TABLE IF NOT EXISTS edges(
        way_id INT,
        from_node INT,
        to_node INT,
        highway TEXT,
        reversed INT,
        PRIMARY KEY(way_id, from_node, to_node),
        FOREIGN KEY(from_node) REFERENCES nodes(node_id) ON DELETE CASCADE,
        FOREIGN KEY(to_node) REFERENCES nodes(node_id) ON DELETE CASCADE
      );
    `);
    const edge_insert_stmt = await this.db.prepare("INSERT OR REPLACE INTO edges(way_id, from_node, to_node, highway, reversed) VALUES(?, ?, ?, ?, ?);");

    this.db.run(`
      CREATE TABLE IF NOT EXISTS tmcs(
        tmc TEXT PRIMARY KEY,
        miles DOUBLE PRECISION,
        f_system INT,
        geojson TEXT
      );
    `);
    const tmc_insert_stmt = await this.db.prepare("INSERT INTO tmcs(tmc, miles, f_system, geojson) VALUES(?, ?, ?, ?);");

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

  async runCheckpointOne() {
    this.logInfo("RUNNING CHECKPOINT ONE");

    const maxNodeIdSql = `
      SELECT MAX(node_id) AS max_node_id
        FROM nodes;
    `;
    const [{ max_node_id }] = this.db.get(maxNodeIdSql);
    let node_id = +max_node_id;
    const getNewNodeId = () => {
      return ++node_id;
    }

    const intersectionEdgesSql = `
      SELECT
          way_id,
          from_node,
          to_node,
          highway,
          reversed,
          'incoming' AS dir
        FROM edges
        WHERE to_node IN (
          SELECT node_id FROM intersections
        )

      UNION

      SELECT
          way_id,
          from_node,
          to_node,
          highway,
          reversed,
          'outgoing' AS dir
        FROM edges
        WHERE from_node IN (
          SELECT node_id FROM intersections
        )
    `;
    this.logInfo("QUERYING INTERSECTION EDGES");
    const intersectionEdges = this.db.all(intersectionEdgesSql);
    const grouped = d3group(intersectionEdges, e => e.dir, e => e.dir === "incoming" ? e.to_node : e.from_node);

    const [
      oldNodeInsertStmt,
      newNodeInsertStmt,
      newEdgeInsertStmt
    ] = await this.initializeSQLiteDBforCheckpointOne();

    const incAmt = 10000;
    let logInfoAt = incAmt;
    let numIntersections = 0;

    const incomingEdges = grouped.get("incoming");
    const outgoingEdges = grouped.get("outgoing");

    const intersectionNodesSql = `
      SELECT *
        FROM nodes
        WHERE node_id IN (SELECT node_id FROM intersections);
    `;
    this.logInfo("QUERYING INTERSECTION NODES");
    const iterator = this.db.prepare(intersectionNodesSql).iterate();

    this.logInfo("PROCESSING INTERSECTIONS");
    for (const { node_id, lon, lat } of iterator) {
      const incoming = incomingEdges.get(node_id);
      const outgoing = outgoingEdges.get(node_id);

      if (incoming && outgoing) {
        oldNodeInsertStmt.run(node_id);

        const outgoingMap = d3group(outgoing, e => e.from_node);
        for (const inEdge of incoming) {
          const outgoing = outgoingMap.get(node_id);
          if (outgoing) {
            const newNodeId = getNewNodeId();
            newNodeInsertStmt.run({
              lon, lat,
              node_id: newNodeId,
              base_node_id: node_id
            });
            for (const outEdge of outgoing) {
              if (outEdge.way_id !== inEdge.way_id) {
                newEdgeInsertStmt.run({
                  ...outEdge,
                  from_node: newNodeId
                })
              }
            }
          }
        }
        if (++numIntersections >= logInfoAt) {
          this.logInfo("PROCESSED", numIntersections, "INTERSECTIONS");
          logInfoAt += incAmt;
        }
      }
    }
  }
  async initializeSQLiteDBforCheckpointOne() {
    this.db.run(`
      CREATE TABLE old_nodes(
        node_id INT PRIMARY KEY
      );
    `);
    const oldNodeInsertStmt = this.db.prepare("INSERT INTO old_nodes(node_id) VALUES(?);");

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
        way_id INT,
        from_node INT,
        to_node INT,
        highway TEXT,
        reversed INT,
        PRIMARY KEY(way_id, from_node, to_node),
        FOREIGN KEY(from_node) REFERENCES new_nodes(node_id) ON DELETE CASCADE,
        FOREIGN KEY(to_node) REFERENCES new_nodes(node_id) ON DELETE CASCADE
      );
    `);
    const newEdgeInsertStmt = this.db.prepare("INSERT INTO new_edges(way_id, from_node, to_node, highway, reversed) VALUES($way_id, $from_node, $to_node, $highway, $reversed)");

    return [
      oldNodeInsertStmt,
      newNodeInsertStmt,
      newEdgeInsertStmt
    ]
  }

  getNodeTransform(node_insert_stmt) {
    const myGraph = this;
    const incAmt = 500000;
    let logInfoAt = incAmt;
    let numNodes = 0;
    const intFormat = d3format(",d");
    return new Transform({
      transform(chunk, encoding, next) {
        const [id, lon, lat] = chunk.toString().split("|").map(Number);
        node_insert_stmt.run(id, lon, lat);
        if (++numNodes >= logInfoAt) {
          myGraph.logInfo("LOADED", intFormat(numNodes), "NODES");
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
    const intFormat = d3format(",d");
    const formatter = d3dsvFormat("|");
    return new Transform({
      transform(chunk, encoding, next) {
        const [parsedRow] = formatter.parseRows(chunk.toString());
        const [id, refs, tags] = parsedRow.map(JSON.parse);
        for (let i = 0; i < refs.length - 1; ++i) {
          // this.graph.addLink(refs[i], refs[i + 1], tags);
          edge_insert_stmt.run(id, refs[i], refs[i + 1], tags.highway, 0);
        }
        if (tags.oneway !== "yes") {
          for (let i = refs.length - 1; i > 1; --i) {
            // this.graph.addLink(refs[i], refs[i - 1], tags);
            edge_insert_stmt.run(id, refs[i], refs[i - 1], tags.highway, 1);
          }
        }
        if (++numWays >= logInfoAt) {
          myGraph.logInfo("LOADED", intFormat(numWays), "WAYS");
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
    const formatter = d3dsvFormat("|");
    return new Transform({
      transform(chunk, encoding, next) {
        const [[tmc, miles, f_system, geojson]] = formatter.parseRows(chunk.toString());
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
  async findIntersections() {
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
    await this.db.run(sql);
  }
  async reportStats() {
    const intFormat = d3format(",d");
    this.logInfo("LOADED", intFormat(this.quadtree.size()), "NODES INTO QUADTREE");
    const nodeCountSql = `
      SELECT COUNT(1) AS count
      FROM nodes
    `;
    const [{ count: nodes }] = await this.db.get(nodeCountSql);
    this.logInfo("LOADED", intFormat(nodes), "NODES INTO SQLITE DB");

    this.logInfo("LOADED", intFormat(this.graph.getNodeCount()), "NODES INTO GRAPH");
    this.logInfo("LOADED", intFormat(this.graph.getEdgeCount()), "EDGES INTO GRAPH");
    const edgeCountSql = `
      SELECT COUNT(1) AS count
      FROM edges
    `;
    const [{ count: edges }] = await this.db.get(edgeCountSql);
    this.logInfo("LOADED", intFormat(edges), "EDGES INTO SQLITE DB");

    const tmcCountSql = `
      SELECT COUNT(1) AS count
      FROM tmcs
    `;
    const [{ count: tmcs }] = await this.db.get(tmcCountSql);
    this.logInfo("LOADED", intFormat(tmcs), "TMCs INTO SQLITE DB");

    const intersectionCountSql = `
      SELECT COUNT(1) AS count
      FROM intersections
    `;
    const [{ count: intersections }] = await this.db.get(intersectionCountSql);
    this.logInfo("LOADED", intFormat(intersections), "INTERSECTIONS INTO SQLITE DB");
  }
  async saveCheckpoint(checkpoint) {
    const filepath = join("./checkpoints", checkpoint);
    await this.saveToDisk(filepath);
  }
  async saveToDisk(filepath, options) {
    await this.db.backup(filepath, options);
  }
  loadFromDisk(filepath) {
    if (existsSync(filepath)) {
      this.logInfo("FILE", filepath, "EXISTS. LOADING SQLITE DB.");
      const db = new SQLite3DB(filepath);
      const buffer = db.serialize();
      return new SQLite3DB(buffer);
    }
    this.logInfo("FILE", filepath, "DOES NOT EXISTS. CREATING NEW SQLITE DB.");
    return new SQLite3DB();
  }
}
