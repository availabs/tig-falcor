const os = require("os");

const { hazmit_db } = require("../db_service");

const hostname = os.hostname();

const pathSetLogThreshold = 1000; //ms

async function logServerRequest(data) {
  try {
    const { responseTimeMs, falcorRoute, falcorPathSet } = data;

    const loggedRoute = falcorRoute.replace(/\[['"].*?['"]\]/g, "[...]");

    const loggedPathSet =
      responseTimeMs >= pathSetLogThreshold
        ? JSON.stringify(falcorPathSet)
        : null;

    const query = {
      name: "server-request-performance-stats",
      text: `
        INSERT INTO admin_performance_monitoring.avail_falcor_requests (
          user_email,
          request_timestamp,
          response_time_ms,
          hostname,
          falcor_route,
          falcor_path_set
        ) VALUES ($1, $2, $3, $4, $5, $6) ;
      `,
      values: [
        data.userEmail,
        data.timestamp,

        data.responseTimeMs,

        hostname,

        loggedRoute,
        loggedPathSet
      ]
    };

    // DO NOT AWAIT. DO NOT WANT TO AFFECT API RESPONSE TIMES.
    hazmit_db.query(query);
  } catch (err) {
    console.error(err);
  }
}

module.exports = { logServerRequest };
