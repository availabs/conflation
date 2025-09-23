import initializeCheckpoint from "./initializeCheckpoint.mjs"
import loadRIS from "./loadRIS.mjs"

async function runCheckpoint(TheConflationator) {
    const { RIS_view_id } = TheConflationator.config;

    const client = await TheConflationator.getClient();

    const [
      ris_insert_stmt
    ] = initializeCheckpoint(TheConflationator);

    const RIS_table = await TheConflationator.getDataTable(client, RIS_view_id);

    await loadRIS(TheConflationator, client, RIS_table, ris_insert_stmt);

    await client.end();
}
export default runCheckpoint;