import { basename, dirname, join } from "node:path";
import { fileURLToPath } from 'node:url';
import {
  existsSync,
  readdirSync,
  copyFileSync,
  rmSync,
  mkdirSync,
  createWriteStream
} from "node:fs"

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const LoaderConfig = {
	name: "Conflationator",
	// function: InitializeConflationator,
	steps: [
		{	name: "DataLoader",
			// function: InitializeDataLoader,
			export: "checkpoint-1",
			steps: [
				{	name: "LoadNodes",
					// function: LoadNodes
				},
				{	name: "LoadWays",
					// function: LoadWays
				},
				{	name: "LoadRelations",
					// function: LoadRelations
				},
				{	name: "LoadTMCs",
					// function: LoadTMCs
				},
				{	name: "FindIntersections",
					// function: FindIntersections
				},
				{	name: "FindConnections",
					// function: FindConnections
				},
				{	name: "ReportStats",
					// function: ReportStatsForDataLoader
				}
			]
		},
		{	name: "GraphGenerator",
			import: "checkpoint-1",
			export: "checkpoint-2",
			// function: InitializeGraphGenerator,
			steps: [
				{	name: "ProcessIntersections",
					// function: ProcessIntersections
				},
				{	name: "ProcessConnections",
					// function: ProcessConnections
				},
				{	name: "ProcessReversedWays",
					// function: ProcessReversedWays
				},
				{	name: "ReportStats",
					// function: ReportStatsForGraphGenerator
				}
			]
		}
	]
}

const CONFIG_LOADER_DEFAULTS = {
	checkpointsDirectory: join(__dirname, "checkpoints"),
	workingDirectory: join(__dirname, "sqlite")
}

const ConfigLoader = config => {
	const LoaderConfig = {
		...CONFIG_LOADER_DEFAULTS,
		...config
	}
	class ConfigLoader {
		constructor() {
			if (!existsSync(LoaderConfig.checkpointsDirectory)) {
				mkdirSync(LoaderConfig.checkpointsDirectory);
			}
			this.checkpointsDirectory = LoaderConfig.checkpointsDirectory;
			
			if (!existsSync(LoaderConfig.workingDirectory)) {
				mkdirSync(LoaderConfig.workingDirectory);
			}
			this.workingDirectory = LoaderConfig.workingDirectory;
		}
		run(steps) {
			steps = steps || [];
			if (!Array.isArray(steps)) {
				steps = [steps];
			}
			if (!steps.length) {
				steps = [this.findLatestCheckpoint()]
			}

			const allStepPaths = this.exploreAllSteps(LoaderConfig);

			const expandedSteps = [];

			for (const step of steps) {
				const path = step.split(".");
				for (const stepPath of allStepPaths) {
					for (let i = --path.length; i < 0; --i) {

					}
				}
			}
		}
		findLatestCheckpoint() {
	    const checkpoints = readdirSync(this.checkpointsDirectory).sort((a, b) => b.localeCompare(a));
	    if (checkpoints.length) {
	    	return basename(checkpoints[0], ".sqlite");
	    }
	    return config.name;
		}
		findFullPath(stepName, config, path) {
			if (config.name === stepName) {
				return [...path, config.name];
			}
			if (!config.steps) {
				return path;
			}
			for (const step of config.steps) {
				const newPath = this.findFullPath(stepName, step, [...path, config.name]);
				if (newPath.at(-1) === stepName) {
					return newPath;
				}
			}
			return path;
		}
		exploreAllSteps(config, paths = [], current = []) {
			paths.push([...current, config.name]);
			if (config.steps) {
				for (const step of config.steps) {
					this.exploreAllSteps(step, paths, [...current, config.name])
				}
			}
			return paths;
		}
	}
	return new ConfigLoader();
}

(() => {
	const configLoader = ConfigLoader(LoaderConfig);

	configLoader.run();
	configLoader.run("DataLoader");
	configLoader.run("LoadTMCs");
	configLoader.run("DataLoader.LoadWays");
	configLoader.run("ReportStats");

	console.log(configLoader.exploreAllSteps(LoaderConfig));
})();