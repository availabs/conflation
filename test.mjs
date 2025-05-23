
import { readdirSync } from "node:fs"

const stuff = readdirSync("./node_modules");
console.log(stuff)
