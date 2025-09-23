export default function initializeCheckpoint(TheConflationator) {

  	TheConflationator.db.run(`
    	CREATE TABLE IF NOT EXISTS nodes(
      		node_id INT PRIMARY KEY,
      		lon DOUBLE PRECISION NOT NULL,
      		lat DOUBLE PRECISION NOT NULL,
      		base_node_id INT
    	);
  	`);
  	const node_insert_stmt = TheConflationator.db.prepare("INSERT INTO nodes(node_id, lon, lat) VALUES(?, ?, ?);");

  	TheConflationator.db.run(`
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
  	const edgeInsertSql = `
  		INSERT OR REPLACE INTO edges(way_id, pos, from_node, to_node, bearing, length, highway, reversed)
  			VALUES(?, ?, ?, ?, ?, ?, ?, ?);
  	`;
  	const edge_insert_stmt = TheConflationator.db.prepare(edgeInsertSql);
  
  	const edgesWayIdIndexSql = `
    	CREATE INDEX edges_way_id_idx
      	ON edges(way_id);
  	`;
  	TheConflationator.db.run(edgesWayIdIndexSql);
  
  	const edgesFromNodeIndexSql = `
    	CREATE INDEX edges_from_node_idx
      	ON edges(from_node);
  	`;
  	TheConflationator.db.run(edgesFromNodeIndexSql);

  	const edgesToNodeIndexSql = `
    	CREATE INDEX edges_to_node_idx
      	ON edges(to_node);
  	`;
  	TheConflationator.db.run(edgesToNodeIndexSql);

  	TheConflationator.db.run(`
    	CREATE TABLE IF NOT EXISTS intersections(
      		node_id INT PRIMARY KEY
    	);
  	`);

  	return [
	    node_insert_stmt,
	    edge_insert_stmt
  	]
}