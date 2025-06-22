import TheConflationator from "./TheConflationator.mjs"

import config from "./config.js"

const main = async () => {

  const theConflationator = new TheConflationator(config);

  try {
    await theConflationator.loadLatestCheckpoint().run();
  }
  catch (e) {
    theConflationator.logInfo("ERROR:\n", JSON.stringify(e, null, 3));
    console.log("ERROR:\n", e);
  }
  finally {
    await theConflationator.finalize();
  }
}

main();
