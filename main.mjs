import { execSync } from 'node:child_process';

import MyGraph from "./MyGraph.mjs"

import config from "./config.js"
const { npmrds2, OSM_view_id, NPMRDS_view_id } = config;

const main = async () => {

  const myGraph = new MyGraph(config);

  await myGraph.loadLatestCheckpoint().run();

  await myGraph.finalize();
}

main();
