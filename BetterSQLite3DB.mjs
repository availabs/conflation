import Database from 'better-sqlite3'

class Statement {
  constructor(stmt) {
    this.stmt = stmt;
  }
  run(...args) {
    this.stmt.run(...args);
    return this;
  }
  get(...args) {
    const row = this.stmt.get(...args);
    return [row];
  }
  all(...args) {
    return this.stmt.all(...args);
  }
  iterate(...args) {
    return this.stmt.iterate(...args);
  }
}

export default class SQLite3DB {
  constructor(filename = ":memory:", options) {
    this.db = new Database(filename, options);
  }
  run(sql, ...args) {
    this.db.prepare(sql).run(...args);
    return this;
  }
  get(sql, ...args) {
    const row = this.db.prepare(sql).get(...args);
    return [row];
  }
  all(sql, ...args) {
    return this.db.prepare(sql).all(...args);
  }
  prepare(...args) {
    return new Statement(this.db.prepare(...args));
  }
  async backup(...args) {
    return this.db.backup(...args);
  }
  serialize(...args) {
    return this.db.serialize(...args);
  }
  close() {
    return this.db.close();
  }
  end() {
    return this.close();
  }
}
