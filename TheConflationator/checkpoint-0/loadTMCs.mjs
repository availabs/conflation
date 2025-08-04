import { pipeline, Transform } from "node:stream";
import split2 from "split2"
import pgCopyStreams from "pg-copy-streams";

import { format as d3format } from "d3-format"
import { dsvFormat as d3dsvFormat } from "d3-dsv"

import * as turf from "@turf/turf";

const d3intFormat = d3format(",d");
const d3dsvFormatter = d3dsvFormat("|");

function getTmcTransform(TheConflationator, tmc_insert_stmt) {

	const incAmt = 10000;
	let logInfoAt = incAmt;
	let numTMCs = 0;

	return new Transform({
	  	transform(chunk, encoding, next) {
		    const [[tmc, f_system, geojson]] = d3dsvFormatter.parseRows(chunk.toString());

		    const mainGeometry = JSON.parse(geojson);

		    const MUST_BREAK_AT = 45.0;
		    const BEARING_LIMIT = 30.0;
		    const BEARING_LENGTH = 0.05;

		    const MAX_SEGMENT_MILES = 0.25;
		    const MIN_SEGMENT_LENGTH = 0.1;

// HANDLE EACH LINESTRING OF THE MULTILINESTRINGS SEPARATELY
		    for (let lsIndex = 0; lsIndex < mainGeometry.coordinates.length; ++lsIndex) {

		      	const coordinates = [...mainGeometry.coordinates[lsIndex]];

		      	const geometry = {
			        type: "LineString",
			        coordinates
		      	}

		      	const totalGeometryMiles = turf.length(geometry, { units: "miles" });

		      	let prevBearing = null;
		      	let length = 0;

		      	const linestringSlices = [0];

// TRAVEL ALONG THE TMC AND BREAK IT AT LARGE ANGLES,
// MAINTAINING ORIGINAL POINTS
		      	for (let i = 1; i < coordinates.length; ++i) {
			        const from = coordinates[i - 1];
			        const to = coordinates[i];
			        const bearing = turf.bearing(from, to);
			        length += turf.distance(from, to, { units: "miles" });
			        if (i === 1) {
			          prevBearing = bearing;
			        }
			        else {
			          if ((Math.abs(prevBearing - bearing) >= MUST_BREAK_AT) ||
			              ((Math.abs(prevBearing - bearing) >= BEARING_LIMIT) &&
			                (length >= BEARING_LENGTH))) {
			            linestringSlices.push(i, i - 1);
			            prevBearing = bearing;
			            length = 0;
			          }
			        }
		      	}
		      	linestringSlices.push(coordinates.length);

		      	const linestringSegments = [];
		      	for (let i = 0; i < linestringSlices.length; i += 2) {
			        const s1 = linestringSlices[i];
			        const s2 = linestringSlices[i + 1];
			        linestringSegments.push(coordinates.slice(s1, s2));
		      	}

		      	let tmcIndex = 0;

		      	for (const lsSegment of linestringSegments) {
		        	const geometry = {
		          		type: "LineString",
		          		coordinates: [...lsSegment]
		        	};
		        	const segmentMiles = turf.length(geometry, { units: "miles" });

// LOOK FOR LARGE TMC CHUNKS AND BREAK THEM INTO SMALLER CHUNKS,
// MAINTAINING ORIGINAL POINTS
		        	if (segmentMiles >= MAX_SEGMENT_MILES) {
		          		const numChunks = Math.ceil(segmentMiles) * 2;
		          		const idealMiles = segmentMiles / numChunks;

		          		let totalMiles = 0.0;
		          		let currentMiles = 0.0;

		          		const newSlices = [0];

		          		for (let i = 1; i < lsSegment.length - 1; ++i) {
				            const from = lsSegment[i - 1];
				            const to = lsSegment[i];
				            const dist = turf.distance(from, to, { units: "miles" });
				            totalMiles += dist;
				            currentMiles += dist;
				            const remainingMiles = segmentMiles - totalMiles;
				            if ((remainingMiles >= MIN_SEGMENT_LENGTH) &&
				                (currentMiles >= idealMiles)) {
				              newSlices.push(i + 1, i);
				              currentMiles = 0.0;
				            }
		          		}
		          		newSlices.push(lsSegment.length);

			          	const newSegments = [];
			          	for (let i = 0; i < newSlices.length; i += 2) {
				            const s1 = newSlices[i];
				            const s2 = newSlices[i + 1];
				            newSegments.push(lsSegment.slice(s1, s2));
			          	}

		          		for (const segment of newSegments) {
		            		const geometry = {
				              	type: "LineString",
				              	coordinates: [...segment]
		            		};
		            		const segmentMiles = turf.length(geometry, { units: "miles" });

// SAVING LINESTRING INDICES AND TMC INDICES ALLOWS FOR RECREATION OF MULTILINESTRINGS
		            		tmc_insert_stmt.run(tmc, lsIndex, tmcIndex, segmentMiles, f_system || 6, JSON.stringify(geometry));
		            		++tmcIndex;
		          		}
			        }
			        else {
			          	tmc_insert_stmt.run(tmc, lsIndex, tmcIndex, segmentMiles, f_system || 6, JSON.stringify(geometry));
			          	++tmcIndex;
			        }
		      	}
		    }
		    if (++numTMCs >= logInfoAt) {
		      TheConflationator.logInfo("LOADED", d3intFormat(numTMCs), "TMCs");
		      logInfoAt += incAmt;
		    }
		    next();
	  	}
	});
}

async function loadTMCs(TheConflationator, client, NPMRDS_table, tmc_insert_stmt) {
	const copyToStream = client.query(
  		pgCopyStreams.to(`
    		COPY (
      			SELECT
        			tmc,
        			f_system,
        			ST_AsGeoJSON(wkb_geometry)
      			FROM ${ NPMRDS_table }
      				ORDER BY TMC
    		)
    		TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
  		`)
	);
	TheConflationator.logInfo("LOADING TMCs");
	await new Promise((resolve, reject) => {
	  	pipeline(
	    	copyToStream,
	    	split2(),
	    	getTmcTransform(TheConflationator, tmc_insert_stmt),
	    	error => {
	      		if (error) {
	        		reject(error);
	      		}
	      		else {
	        		resolve();
	      		}
	    	}
	  	)
	})
}
export default loadTMCs;