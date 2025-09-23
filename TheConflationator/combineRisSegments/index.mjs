import combineSegments from "../combineSegments.mjs"

import initializeCheckpoint from "./initializeCheckpoint.mjs"

const runCheckpoint = combineSegments.bind(null, initializeCheckpoint);

export default runCheckpoint;