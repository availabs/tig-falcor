// https://github.com/brianc/node-postgres/blob/947ccee346f0d598e135548e1e4936a9a008fc6f/packages/pg-pool/index.js#L343-L394

const { createWriteStream } = require("fs");

const { npmrds_db, hazmit_db, tig_db } = require("../../../db_service");

let i = 0;

function interceptQueries(db, logger) {
  const { database } = db;

  const _db_query = db.query.bind(db);

  db.query = async (...args) => {
    let [text, values, cb] = args;

    if (typeof values === "function") {
      cb = values;
      values = undefined;
    }

    const start = process.hrtime();

    let result = null;
    try {
      result = await _db_query(text, values);
    } catch (err) {
      console.error(err);

      if (typeof cb === "function") {
        return cb(err);
      }

      throw err;
    }

    const end = process.hrtime(start);

    const responseTimeMs = (end[0] * 1000000000 + end[1]) / 1000000;

    logger.write(
      `${JSON.stringify({ i: i++, database, text, values, responseTimeMs })}\n`
    );

    return typeof cb === "function" ? cb(null, result) : result;
  };

  db.promise = async (...args) => {
    const { rows } = await db.query(...args);
    return rows;
  };
}

module.exports = function logDbPerformance(queryLogFilePath) {
  const logger = createWriteStream(queryLogFilePath);

  interceptQueries(npmrds_db, logger);
  interceptQueries(hazmit_db, logger);
  interceptQueries(tig_db, logger);
};
