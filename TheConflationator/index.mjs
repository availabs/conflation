
import {
  existsSync,
  readdirSync,
  copyFileSync,
  rmSync,
  mkdirSync
} from "node:fs"

import { join, dirname, basename } from "node:path";
import { fileURLToPath } from 'node:url';

import SQLite3DB from "./BetterSQLite3DB.mjs"

import RunCheckpointZero from "./checkpoint-0/index.mjs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const WORKING_DIRECTORY = join(__dirname, "working_directory");
const ACTIVE_DB_PATH = join(WORKING_DIRECTORY, "active_db.sqlite");

const CHECKPOINTS_DIRECTORY = join(__dirname, "checkpoints");
const CHECKPOINT_REGEX = /^checkpoint[-]\d+.sqlite$/;

export default class TheConflationator {
  constructor(config) {
    this.config = { ...config };
    this.checkpoint = null

    this.db = null;
  }
  async initialize() {
    if (!existsSync(CHECKPOINTS_DIRECTORY)) {
      mkdirSync(CHECKPOINTS_DIRECTORY);
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
    const [latestCheckpoint] = readdirSync(CHECKPOINTS_DIRECTORY)
                          .filter(f => CHECKPOINT_REGEX.test(f))
                          .sort((a, b) => b.localeCompare(a));
    if (latestCheckpoint) {
      this.checkpoint = basename(latestCheckpoint, ".sqlite");
      this.db = this.loadFromDisk(join(CHECKPOINTS_DIRECTORY, latestCheckpoint));
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
    const filepath = join(CHECKPOINTS_DIRECTORY, `${ checkpoint }.sqlite`);
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
    await RunCheckpointZero(this);

    await this.saveCheckpoint("checkpoint-0");

    return this.runCheckpointOne();
  }
  async runCheckpointOne() {
  }
  async runCheckpointTwo() {
  }
  async runCheckpointThree() {
  }
}