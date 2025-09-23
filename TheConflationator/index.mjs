import {
  existsSync,
  readdirSync,
  copyFileSync,
  rmSync,
  mkdirSync
} from "node:fs"

import { join, dirname, basename } from "node:path";
import { fileURLToPath } from 'node:url';

import pgStuff from "pg";

import SQLite3DB from "./BetterSQLite3DB.mjs"

import runLoadNodesAndWays from "./loadNodesAndWays/index.mjs"
import runGenerateDirectedGraph from "./generateDirectedGraph/index.mjs"

import runLoadTMCs from "./loadTMCs/index.mjs"
import runProcessTmcSegments from "./processTmcSegments/index.mjs"
import runCombineTmcSegments from "./combineTmcSegments/index.mjs"

import runLoadRis from "./loadRIS/index.mjs"
import runProcessRisSegments from "./processRisSegments/index.mjs"
import runCombineRisSegments from "./combineRisSegments/index.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const WORKING_DIRECTORY = join(__dirname, "working_directory");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

const CHECKPOINTS_DIRECTORY = join(__dirname, "checkpoints");
const TMC_CHECKPOINTS_DIRECTORY = join(__dirname, "TMC_checkpoints");
const RIS_CHECKPOINTS_DIRECTORY = join(__dirname, "RIS_checkpoints");
const CHECKPOINT_REGEX = /^checkpoint[-]\d{1}.?\d?.sqlite$/;

export default class TheConflationator {
  constructor(config, network = "NONE") {
    this.config = { ...config };

    this.db = { close: () => {} };

    this.checkpoint = null;

    this.checkpoints = [
      { cp: "checkpoint-0", func: runLoadNodesAndWays },
      { cp: "checkpoint-1", func: runLoadTMCs },
      { cp: "checkpoint-2", func: runLoadRis },
      { cp: "checkpoint-3", func: runProcessTmcSegments },
      { cp: "checkpoint-4", func: runCombineTmcSegments },
      { cp: "checkpoint-5", func: runProcessRisSegments },
      { cp: "checkpoint-6", func: runCombineRisSegments },
    ].map(({ cp, func }) => ({ cp, func: func.bind(this, this, cp) }), this);

    this.TMCcheckpoints = [
      { cp: "checkpoint-0", func: runLoadNodesAndWays },
      { cp: "checkpoint-1", func: runLoadTMCs },
      { cp: "checkpoint-2", func: runProcessTmcSegments },
      { cp: "checkpoint-3", func: runCombineTmcSegments },
    ].map(({ cp, func }) => ({ cp, func: func.bind(this, this, cp) }), this);

    this.RIScheckpoints = [
      { cp: "checkpoint-0", func: runLoadNodesAndWays },
      { cp: "checkpoint-1", func: runLoadRis },
      { cp: "checkpoint-2", func: runProcessRisSegments },
      { cp: "checkpoint-3", func: runCombineRisSegments },
    ].map(({ cp, func }) => ({ cp, func: func.bind(this, this, cp) }), this);

    this.checkpoints = network === "TMC" ? this.TMCcheckpoints :
                        network === "RIS" ? this.RIScheckpoints :
                                            this.checkpoints;

    this.CHECKPOINTS_DIRECTORY = network === "TMC" ? TMC_CHECKPOINTS_DIRECTORY :
                                  network === "RIS" ? RIS_CHECKPOINTS_DIRECTORY :
                                                      CHECKPOINTS_DIRECTORY;
  }
  async initialize() {
    if (!existsSync(this.CHECKPOINTS_DIRECTORY)) {
      mkdirSync(this.CHECKPOINTS_DIRECTORY);
    }
  }
  async finalize() {
    await this.db.close();
    this.removeWorkingDirectory();
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
  async getClient() {
    this.logInfo("CONNECTING PG CLIENT");
    const client = new pgStuff.Client(this.config.db_info);
    await client.connect();
    return client;
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
  removeWorkingDirectory() {
    rmSync(WORKING_DIRECTORY, { force: true, recursive: true });
  }
  prepWorkingDirectory() {
    this.removeWorkingDirectory();
    mkdirSync(WORKING_DIRECTORY);
  }
  loadLatestCheckpoint() {
    this.prepWorkingDirectory();
    const [latestCheckpoint] = readdirSync(this.CHECKPOINTS_DIRECTORY)
                          .filter(f => CHECKPOINT_REGEX.test(f))
                          .sort((a, b) => b.localeCompare(a));
    if (latestCheckpoint) {
      this.checkpoint = basename(latestCheckpoint, ".sqlite");
      this.db = this.loadFromDisk(join(this.CHECKPOINTS_DIRECTORY, latestCheckpoint));
      this.logInfo("LOADED CHECKPOINT:", this.checkpoint);
    }
    else {
      this.db = new SQLite3DB(ACTIVE_DB_PATH);
      this.logInfo("NO CHECKPOINT FOUND. CREATING NEW SQLITE DB.");
    }
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('cache_size = -4096');
    return this;
  }
  async saveCheckpoint(checkpoint) {
    this.logInfo("SAVING CHECKPOINT", checkpoint, "TO DISK");
    const filepath = join(this.CHECKPOINTS_DIRECTORY, `${ checkpoint }.sqlite`);
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
    const index = this.checkpoints.findIndex(cp => cp.cp === this.checkpoint, this);

    for (let i = index + 1; i < this.checkpoints.length; ++i) {
      const { cp, func } = this.checkpoints[i];
      this.checkpoint = cp;
      this.logInfo("RUNNING CHECKPOINT:", cp);
      await func();
      await this.saveCheckpoint(cp);
    }

    this.logInfo("COMPLETED ALL CHECKPOINTS");
  }
}