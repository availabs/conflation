import processSegments from "../processSegments.mjs"

import initializeCheckpoint from "./initializeCheckpoint.mjs"

const runCheckpoint = processSegments.bind(null, initializeCheckpoint);

export default runCheckpoint;