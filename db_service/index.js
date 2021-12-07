const { join } = require("path"),
  { Pool, Client } = require("pg");

const NPMRDS_CONFIG = require(join(__dirname, "npmrds.config.json")),
  HAZMIT_CONFIG = require(join(__dirname, "hazmit.config.json")),
  TIG_CONFIG = require(join(__dirname,"tig.config.json"));

class DataBase {
  constructor(config) {
    this.database = config.database
    this.pool = new Pool(config);
  }
  query(...args) {
    return this.pool.query(...args)
  }
  end() {
    return this.pool.end();
  }
  promise(...args) {

    return new Promise((resolve, reject) => {
      this.pool.query(...args, (error, result) => {
        if (error) {
          console.log(`<DataBase> ${ this.database } ERROR:`, ...args, error);
          reject(error);
        }
        else {
          resolve(result.rows);
        }
      })
    })
  }
}

const npmrdsClient = async () => {
  const client = new Client(NPMRDS_CONFIG);
  await client.connect();
  return client;
};

const DATABASES = {
  npmrds_db: new DataBase(NPMRDS_CONFIG),
  hazmit_db: new DataBase(HAZMIT_CONFIG),
  tig_db: new DataBase(TIG_CONFIG)
};

module.exports = {
  npmrdsClient,
  ...DATABASES,
  end: arg => {
    if (Array.isArray(arg)) {
      arg.forEach(db => DATABASES[db] && DATABASES[db].end());
    }
    else if (typeof arg === "string") {
      DATABASES[arg] && DATABASES[arg].end();
    }
    else {
      for (const db in DATABASES) {
        DATABASES[db].end();
      }
    }
  }
}
