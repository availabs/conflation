import { format as d3format } from "d3-format"
import { scaleLinear as d3scaleLinear } from "d3-scale"

export const D3_INT_FORMAT = d3format(",d");

export const MAX_BEARING_RANK = 120.0;
export const BEARING_SCALE = d3scaleLinear([0.0, 45.0], [MAX_BEARING_RANK, 0]);

export const HIGHWAY_TO_F_SYSTEM_MAP = {
  motorway: 1.0,
  motorway_link: 1.5,

  trunk: 2.0,
  trunk_link: 2.5,

  primary: 3.0,
  primary_link: 3.5,

  secondary: 4.0,
  secondary_link: 4.5,

  tertiary: 5.0,
  tertiary_link: 5.5,

  unclassified: 6.0,

  residential: 7.0,
  living_street: 7.0
}