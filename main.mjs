import TheConflationator from "./TheConflationator/index.mjs"

import config from "./config.js"

(async () => {

  const theConflationator = new TheConflationator(config);
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
