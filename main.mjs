import { argv } from "node:process"

import TheConflationator from "./TheConflationator/index.mjs"

import config from "./config.js"

(async () => {

  const network = argv[2];

  const theConflationator = new TheConflationator(config, network);
  await theConflationator.initialize();

  try {
    await theConflationator.loadLatestCheckpoint().run();
  }
  catch (e) {
    theConflationator.logInfo("ERROR:\n", e.message || e);
    if (e.stack) {
      theConflationator.logInfo(e.stack);
    }
  }
  finally {
    await theConflationator.finalize();
  }
})();
