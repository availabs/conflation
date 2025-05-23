import sqlite3 from 'sqlite3'

class Statement {
  constructor(stmt) {
    this.stmt = stmt;
  }
  run(...args) {
    return new Promise((resolve, reject) => {
      this.stmt.run(...args, error => { if (error) { reject(error); } else { resolve(); } });
    });
  }
  get(...args) {
    return new Promise((resolve, reject) => {
      this.stmt.get(...args, (error, row) => { if (error) { reject(error); } else { resolve([row]); } });
    });
  }
  all(...args) {
    return new Promise((resolve, reject) => {
      this.stmt.all(...args, (error, rows) => { if (error) { reject(error); } else { resolve(rows); } });
    });
  }
  finalize() {
    return new Promise((resolve, reject) => {
      this.stmt.finalize(error => { if (error) { reject(error); } else { resolve(); } });
    });
  }
}

export default class SQLite3DB {
  constructor(filename = ":memory:") {
    this.db = new sqlite3.Database(filename);
  }
  run(...args) {
    return new Promise((resolve, reject) => {
      this.db.run(...args, error => { if (error) { reject(error); } else { resolve(); } });
    });
  }
  get(...args) {
    return new Promise((resolve, reject) => {
      this.db.get(...args, (error, row) => { if (error) { reject(error); } else { resolve([row]); } });
    });
  }
  all(...args) {
    return new Promise((resolve, reject) => {
      this.db.all(...args, (error, rows) => { if (error) { reject(error); } else { resolve(rows); } });
    });
  }
  prepare(...args) {
    return new Promise((resolve, reject) => {
      const stmt = this.db.prepare(...args, error => { if (error) { reject(error); } });
      resolve(new Statement(stmt));
    });
  }
  close() {
    return new Promise((resolve, reject) => {
      this.db.close(error => { if (error) { reject(error); } else { resolve(); } });
    });
  }
  end() {
    return this.close();
  }
}
