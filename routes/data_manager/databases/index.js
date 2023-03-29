const { existsSync } = require("fs");
const {
  readFile: readFileAsync,
  readdir: readdirAsync,
} = require("fs/promises");
const { join } = require("path");

// https://node-postgres.com/api/pool
const { Pool } = require("pg");

const configDir = join(__dirname, "../../../db_service");

const databases = {};

const configFileSuffixRE = /\.config\.json$/;

async function getPostgresCredentials(pgEnv) {
  const configFileName = `${pgEnv}.config.json`;

  const configFilePath = join(configDir, configFileName);

  try {
    const str = await readFileAsync(configFilePath, { encoding: "utf8" });
    const creds = JSON.parse(str);

    return creds;
  } catch (err) {
    if (err.code === "ENOENT") {
      throw new Error(`No configuration file found for pgEnv ${pgEnv}`);
    }

    throw err;
  }
}

async function listPgEnvs() {
  const files = await readdirAsync(configDir);

  const envs = files
    .filter((f) => configFileSuffixRE.test(f))
    .map((f) => f.replace(configFileSuffixRE, ""))
    .sort();

  return envs;
}

async function getDb(pgEnv) {
  if (databases[pgEnv]) {
    return databases[pgEnv];
  }

  const creds = await getPostgresCredentials(pgEnv);

  databases[pgEnv] = new Pool(creds);

  return databases[pgEnv];
}

module.exports = {
  listPgEnvs,
  getDb,
};
