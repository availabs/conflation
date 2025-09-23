import pgStuff from "pg";

import { D3_INT_FORMAT } from "../constants.mjs"

import initializeCheckpoint from "./initializeCheckpoint.mjs"
import loadTMCs from "./loadTMCs.mjs"

async function runCheckpoint(TheConflationator) {
  const { NPMRDS_view_id } = TheConflationator.config;

  const client = await TheConflationator.getClient();

  const [
    tmc_insert_stmt
  ] = initializeCheckpoint(TheConflationator);

  const NPMRDS_table = await TheConflationator.getDataTable(client, NPMRDS_view_id);

  await loadTMCs(TheConflationator, client, NPMRDS_table, tmc_insert_stmt);

  await client.end();
}
export default runCheckpoint;