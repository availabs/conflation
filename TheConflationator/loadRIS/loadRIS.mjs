import { pipeline, Transform } from "node:stream";
import split2 from "split2"
import pgCopyStreams from "pg-copy-streams";

import * as turf from "@turf/turf";

import { format as d3format } from "d3-format"
import { dsvFormat as d3dsvFormat } from "d3-dsv"

const d3intFormat = d3format(",d");
const d3dsvFormatter = d3dsvFormat("|");

function getRisTransform(TheConflationator, ris_insert_stmt) {

	const incAmt = 50000;
	let logInfoAt = incAmt;
	let numRIS = 0;

	return new Transform({
	  	transform(chunk, encoding, next) {
		    const [[ris_id, f_system, direction, divided, one_way, geojson]] = d3dsvFormatter.parseRows(chunk.toString());

		    // const needsReverse = (+direction === 2) || (+direction === 3);

		    const mainGeometry = JSON.parse(geojson);

		    const MUST_BREAK_AT = 30.0;
		    const MAX_SEGMENT_MILES = 0.25;
		    const MIN_SEGMENT_LENGTH = MAX_SEGMENT_MILES * 0.5;

// HANDLE EACH LINESTRING OF THE MULTILINESTRINGS SEPARATELY
		    for (let lsIndex = 0; lsIndex < mainGeometry.coordinates.length; ++lsIndex) {

		      	const coordinates = [...mainGeometry.coordinates[lsIndex]];
		      	// if (needsReverse) {
		      	// 	coordinates.reverse();
		      	// }

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
		      	if (totalGeometryMiles > MIN_SEGMENT_LENGTH) {
			      	for (let i = 1; i < coordinates.length; ++i) {
				        const from = coordinates[i - 1];
				        const to = coordinates[i];
				        const bearing = turf.bearing(from, to);
				        length += turf.distance(from, to, { units: "miles" });
				        if (i === 1) {
				          prevBearing = bearing;
				        }
				        else {
				          if ((Math.abs(prevBearing - bearing) >= MUST_BREAK_AT) &&
				          		(length >= MIN_SEGMENT_LENGTH)) {
				            linestringSlices.push(i, i - 1);
				            prevBearing = bearing;
				            length = 0;
				          }
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

		      	let ris_index = 0;

		      	for (const lsSegment of linestringSegments) {
		        	const geometry = {
		          		type: "LineString",
		          		coordinates: [...lsSegment]
		        	};
		        	const segmentMiles = turf.length(geometry, { units: "miles" });

// LOOK FOR LARGE TMC CHUNKS AND BREAK THEM INTO SMALLER CHUNKS,
// MAINTAINING ORIGINAL POINTS
		        	if (segmentMiles >= MAX_SEGMENT_MILES) {
	          		const numChunks = Math.ceil(segmentMiles / MIN_SEGMENT_LENGTH);
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
			            // if (currentMiles >= idealMiles) {
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
// ris_id, linestring_index, ris_index, miles, f_system, geojson
	            		ris_insert_stmt.run(ris_id, lsIndex, ris_index++, segmentMiles, f_system, JSON.stringify(geometry));
	          		}
			        }
			        else {
		          	ris_insert_stmt.run(ris_id, lsIndex, ris_index++, segmentMiles, f_system, JSON.stringify(geometry));
			        }
		      	}
		    }
		    if (++numRIS >= logInfoAt) {
		      TheConflationator.logInfo("LOADED", d3intFormat(numRIS), "RIS ROADWAYS");
		      logInfoAt += incAmt;
		    }
		    next();
	  	}
	});
}

// const F_CLASS_TO_F_SYSTEM_MAP = {
//   11: 1,
//   12: 2,
//   14: 3,
//   16: 4,
//   17: 5,
//   18: 6,
//   19: 7
// }

async function loadRIS(TheConflationator, client, RIS_table, ris_insert_stmt) {
	const copyToStream = client.query(
  		pgCopyStreams.to(`
    		COPY (
    			SELECT
      			gis_id::TEXT || '-' || beg_mp::TEXT,
  					CASE
  						WHEN functional_class = 1 THEN 1
  						WHEN functional_class = 2 THEN 2
  						WHEN functional_class = 4 THEN 3
  						WHEN functional_class = 6 THEN 4
  						WHEN functional_class = 7 THEN 5
  						WHEN functional_class = 8 THEN 6
  						WHEN functional_class = 9 THEN 7

  						WHEN functional_class = 11 THEN 1
  						WHEN functional_class = 12 THEN 2
  						WHEN functional_class = 14 THEN 3
  						WHEN functional_class = 16 THEN 4
  						WHEN functional_class = 17 THEN 5
  						WHEN functional_class = 18 THEN 6
  						WHEN functional_class = 19 THEN 7
  						ELSE 6
  					END,
  					direction,
		  			divided,
		  			one_way,
      			ST_AsGeoJSON(wkb_geometry)
    			FROM ${ RIS_table }
    		)
    		TO STDOUT WITH (FORMAT CSV, DELIMITER '|')
  		`)
	);
	TheConflationator.logInfo("LOADING RIS");
	await new Promise((resolve, reject) => {
  	pipeline(
    	copyToStream,
    	split2(),
    	getRisTransform(TheConflationator, ris_insert_stmt),
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
export default loadRIS;