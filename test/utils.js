const _ = require('lodash')

const falcorGraph = require('./graph');

const getQueryParams = paths => ({
  queryStringParameters: {
    paths,
    method: 'get'
  }
});

const getFromResponse = (response, path) =>
  _.get(response, ['jsonGraph', ...path], null);

const getFromFalcor = async (reqPath, resPath) =>
  getFromResponse(await falcorGraph.respond(getQueryParams(reqPath)), resPath);

module.exports = {
  getFromFalcor
}
