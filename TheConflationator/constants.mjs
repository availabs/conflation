import { format as d3format } from "d3-format"
import { scaleLinear as d3scaleLinear } from "d3-scale"

export const D3_INT_FORMAT = d3format(",d");

export const MAX_BEARING_RANK = 120.0;
export const MAX_F_SYSTEM_RANK = 120.0;

export const BEARING_SCALE = d3scaleLinear([0.0, 45.0, 90.0], [MAX_BEARING_RANK, 0, -MAX_F_SYSTEM_RANK]);

export const MAX_MILES_RANK = 1.0;
export const MIN_MILES_TO_RANK = 0.5;

export const FEET_TO_MILES = 1.0 / 5280.0;

export const MAX_FEET = 50.0;
export const MIN_FEET = 25.0;
export const BUFFER_SCALE = d3scaleLinear([1, 7], [MAX_FEET * FEET_TO_MILES, MIN_FEET * FEET_TO_MILES]);

export const NUM_EDGES_TO_KEEP = 2;
export const NUM_RESULTS_TO_KEEP = 3;

export const MAX_NUM_GROWTHS = 4;
export const GROW_BACKWARD = -1;
export const GROW_FORWARD = 1;
export const GROWTH_AMOUNT = 5.0 * FEET_TO_MILES;

export const NUM_GAHTER_PASSES = 5;
export const QUALITY_RANK_FILTER = (MAX_BEARING_RANK + MAX_F_SYSTEM_RANK) * 0.9;
export const MIN_RANK_TO_KEEP = (MAX_BEARING_RANK + MAX_F_SYSTEM_RANK) * 0.75;

export const DISTANCE_THRESHOLD = 5.0 / 5280.0;
export const LOOK_BEHIND = -1;
export const LOOK_AHEAD = 1;

export const DISTANCE_RELATIVE_CHANGE = 0.05;
export const DISTANCE_DIFFERENCE = 250 * FEET_TO_MILES;

export const NO_OP = () => {};
export const NO_STMT = {
  run : NO_OP,
  all: NO_OP,
  get: NO_OP
}

export const HIGHWAY_TO_F_SYSTEM_MAP = {
  motorway: 1.0,
  motorway_link: 1.0,

  trunk: 2.0,
  trunk_link: 2.0,

  primary: 3.0,
  primary_link: 3.0,

  secondary: 4.0,
  secondary_link: 4.0,

  tertiary: 5.0,
  tertiary_link: 5.0,

  unclassified: 6.0,

  residential: 7.0,
  living_street: 7.0
}

export const HIGHWAY_TO_DIV_LENGTH_MAP = {
  motorway: 100.0 * FEET_TO_MILES,
  motorway_link: 100.0 * FEET_TO_MILES,

  trunk: 100.0 * FEET_TO_MILES,
  trunk_link: 100.0 * FEET_TO_MILES,

  primary: 75.0 * FEET_TO_MILES,
  primary_link: 75.0 * FEET_TO_MILES,

  secondary: 75.0 * FEET_TO_MILES,
  secondary_link: 75.0 * FEET_TO_MILES,

  tertiary: 50.0 * FEET_TO_MILES,
  tertiary_link: 50.0 * FEET_TO_MILES,

  unclassified: 25.0 * FEET_TO_MILES,

  residential: 20.0 * FEET_TO_MILES,
  living_street: 20.0 * FEET_TO_MILES
}