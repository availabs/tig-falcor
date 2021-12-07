const { readdirSync } = require('fs'),
  { join } = require("path");
  
const regex = /^.+\.app\.js$/;

const routes = readdirSync(__dirname)
  .filter(file => regex.test(file))
  .reduce((routes, file) => routes.concat(require(join(__dirname, file))), []);

module.exports = routes;
