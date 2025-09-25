
const logInfo = (...args) => {
	const string = args.reduce((a, c) => {
	  if (typeof c === "object") {
	    return `${ a } ${ JSON.stringify(c) }`;
	  }
	  return `${ a } ${ c }`
	}, `${ new Date().toLocaleString() }:`);
	console.log(string);
}

async function setDamaTables(client, name, data_type, data_table, categories = []) {

	const [table_schema, table_name] = data_table.split(".");

	const deleteViewSql = `
		DELETE FROM data_manager.views
			WHERE source_id IN (
				SELECT source_id
					FROM data_manager.sources
					WHERE name = $1
			);
	`;
	await client.query(deleteViewSql, [name]);

	const deleteSourceSql = `
		DELETE FROM data_manager.sources
			WHERE name = $1;
	`;
	await client.query(deleteSourceSql, [name]);

	const insertSourceSql = `
		INSERT INTO data_manager.sources(name, type, categories)
			VALUES($1, $2, $3)
		RETURNING source_id;
	`;
	let values = [name, data_type, JSON.stringify(categories)];
	const { rows: [{ source_id }] } = await client.query(insertSourceSql, values);
	logInfo("GOT NEW SOURCE ID:", source_id);

	const insertViewSql = `
		INSERT INTO data_manager.views(source_id, data_type, table_schema, table_name, data_table)
			VALUES($1, $2, $3, $4, $5)
		RETURNING view_id;
	`;
	values = [source_id, data_type, table_schema, table_name, data_table];
	const { rows: [{ view_id }] } = await client.query(insertViewSql, values);
	logInfo("GOT NEW VIEW ID:", view_id);

	const statisticsSql = `
		UPDATE data_manager.sources
			SET statistics = '{"auth": {"users": {}, "groups": {"AVAIL": "10", "public": "2"}}}'
			WHERE source_id = $1;
	`;
	await client.query(statisticsSql, [source_id]);

	const initMetadataSql = `
		CALL _data_manager_admin.initialize_dama_src_metadata_using_view($1)
	`;
	await client.query(initMetadataSql, [view_id]);

	const tilesName = `npmrds2_s${ source_id }_v${ view_id }`;
	const tilesMetadata = {
		tiles: {
  		"sources": [
  			{	"id": tilesName,
  				"source": {
  					"tiles": [`https://graph.availabs.org/dama-admin/npmrds2/tiles/${ view_id }/{z}/{x}/{y}/t.pbf`],
  					"format": "pbf",
  					"type": "vector"
  				}
  			}
  		],
  		"layers": [
  			{	"id": `${ source_id }_v${ view_id }_polygons`,
  				"type": "line",
  				"source": tilesName,
  				"source-layer": `view_${ view_id }`,
  				"paint": {
  					"line-offset": 1.25
  				}
  			}
  		]
  	}
	};

	const updateMetadataWithTilesSql = `
		UPDATE data_manager.views
			SET metadata = COALESCE(metadata, '{}') || '${ JSON.stringify(tilesMetadata) }'::JSONB
			WHERE view_id = $1;
	`;
	await client.query(updateMetadataWithTilesSql, [view_id]);
}
export default setDamaTables;