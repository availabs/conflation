import MyGraph from "./MyGraph.mjs"

import config from "./config.js"

const main = async () => {

  const myGraph = new MyGraph(config);

  try {
    await myGraph.loadLatestCheckpoint().run();
  }
  catch (e) {
    myGraph.logInfo("ERROR:\n", JSON.stringify(e, null, 3));
    console.log("ERROR:\n", e);
  }
  finally {
    await myGraph.finalize();
  }
}

main();
