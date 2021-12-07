const { readdirSync, statSync } = require('fs'),
    { join } = require("path");

const Router = require('../utils/falcor-router');
//require("@graphistry/falcor-router");

const { logServerRequest } = require("../performance/serverRequestLogger");

const regex = /^.+\.route\.js$/;

// const routes = readdirSync(__dirname)
//     .filter(file => regex.test(file))
//     .reduce((routes, file) => routes.concat(require(join(__dirname, file))), []);

const getAllFiles = function(dirPath, arrayOfFiles) {
    let files = readdirSync(dirPath)

    arrayOfFiles = arrayOfFiles || []

    files.forEach(function(file) {
        if (statSync(dirPath + "/" + file).isDirectory()) {

            arrayOfFiles = getAllFiles(dirPath + "/" + file, arrayOfFiles)
        } else {
        	arrayOfFiles.push(join(dirPath, file))
        }
    })

    return arrayOfFiles
}

const result = getAllFiles(__dirname)
    .filter(file => regex.test(file))
    .reduce((routes, file) => routes.concat(require(file)), []);

const logRoutePerformance =
  +process.env.AVAIL_LOG_API_PERFORMANCE > 0 ||
  process.env.AVAIL_LOG_API_PERFORMANCE === "true";

if (logRoutePerformance) {
  console.log("Logging API Performance.");
}

function methodSummary(summary) {
  summary.routes.forEach((routeSummary) => {
    const timestamp = new Date(routeSummary.start);
    const responseTimeMs = Math.round(routeSummary.end - routeSummary.start);

    logServerRequest({
      userEmail: (this.user && this.user.email) || null,
      timestamp,
      responseTimeMs,
      falcorRoute: routeSummary.route,
      falcorPathSet: routeSummary.pathSet,
    });
  });
}

const BaseRouter = Router.createClass(result);

class AVAILRouter extends BaseRouter {
  constructor(config) {
    super({
      maxPaths: 400000,
      hooks: {
        methodSummary: logRoutePerformance ? methodSummary.bind(config) : null,
      },
    });

    this.user = config.user;
  }
}

module.exports = (config = {}) => new AVAILRouter(config);
