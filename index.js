// index.js
const http = require('http')
const cluster = require('cluster');

const falcorExpress = require('falcor-express');
const express = require('express');
const AVAILRouter = require('./routes');

const appRoutes = require("./routes/appRoutes")
const img = require('./routes/image')
const jwtAuth = require('./services/authorization/jwtAuth');
var compression = require('compression')


const {loadConflationRoutingData} = require('./services/conflationRoutingController');

// const imgNew = require("./routes/img")

const app = express();
app.use(compression())

const {
  AUTH_URI,
  AUTH_PROJECT_NAME,
  PORT = 4445,
  NUM_WORKERS = 3
} = require('./server-config');

// Possibly override default config with ENV variables.
const AVAIL_FALCOR_PORT = +process.env.AVAIL_FALCOR_PORT || PORT
const AVAIL_FALCOR_NUM_WORKERS = +process.env.AVAIL_FALCOR_NUM_WORKERS || NUM_WORKERS
const START_CONFLATION_GRAPH = Boolean(process.env.START_CONFLATION_GRAPH || false);
/**
 * Setup number of worker processes to share port which will be defined while setting up server
 */
const setupWorkerProcesses = () => {
  // to read number of cores on system
  console.log(`Master cluster setting up ${  AVAIL_FALCOR_NUM_WORKERS  } workers`);

  // iterate on number of cores need to be utilized by an application
  // current example will utilize all of them
  for (let i = 0; i < AVAIL_FALCOR_NUM_WORKERS; i++) {
    // creating workers and pushing reference in an array
    // these references can be used to receive messages from workers
    const worker = cluster.fork();
    // to receive messages from worker process
    worker.on('message', function (message) {
      console.log(message);
    });

  }

  // process is clustered on a core and process id is assigned
  cluster.on('online', function (worker) {
    console.log(`Worker ${  worker.process.pid  } is listening`);
  });

  // if any of the worker process dies then start a new one by simply forking another one
  cluster.on('exit', function (worker, code, signal) {
    console.log(
      `Worker ${
      worker.process.pid
      } died with code: ${
      code
      }, and signal: ${
      signal}`
    );
    console.log('Starting a new worker');
    const newWorker = cluster.fork();
    // to receive messages from worker process
    newWorker.on('message', function (message) {
      console.log(message);
    });
  });
};

/**
 * Setup an express server and define port to listen all incoming requests for this application
 */
const setUpExpress = () => {

  app.use(express.raw({ limit: "50mb", type: "application/octet-stream" }));

  // app.use(express.text({ limit: "500mb", type: "text/*" }));

  app.use(express.json({ limit: "500mb" })); // to support JSON-encoded bodies
  app.use(express.urlencoded({
    limit: "10mb", extended: true,
    types: ["application/x-www-form-urlencoded", "multipart/form-data"]
  })); // to support URL-encoded bodies

  app.use(function (req, res, next) {
    res.header('Access-Control-Allow-Origin', req.get('origin'));
    // res.header('Access-Control-Allow-Origin', 'https://npmrds.availabs.org');
    // console.log('req origin', req.get('origin'))
    res.header('Cache-Control', 'no-store,no-cache,must-revalidate');
    res.header('Access-Control-Allow-Credentials', true);
    res.header(
      'Access-Control-Allow-Headers',
      'Origin, X-Requested-With, Content-Type, Accept, Authorization'
    );

    res.header(
      'Access-Control-Allow-Methods',
      'GET,PUT,POST,DELETE,PATCH,OPTIONS'
    );

    if (req.method === 'OPTIONS') {
      return res.end()
    }

    return next();
  });

  app.use(jwtAuth);

  appRoutes.forEach(({ method, routes, handler }) => {
    routes.forEach(route => app[method](route, handler));
  })
  // app.post("/img/new/upload/:filename", imgNew.upload);

  app.use(
    '/graph',
    falcorExpress.dataSourceRoute(function (req, res) {
      const {user = null} = req.availAuthContext || {};
      return AVAILRouter({user});
    })
  );

  app.use(express.static(`${__dirname  }/www`));
  app.use('/img/upload', img.upload);
  app.use('/img/process', img.process);

  app.listen(AVAIL_FALCOR_PORT);
};

/**
 * Setup server either with clustering or without it
 * @param isClusterRequired
 */
const setupServer = async isClusterRequired => {
  // if it is a master process then call setting up worker process
  if (isClusterRequired && cluster.isMaster) {
    console.log(new Date())
    console.log('Request MAX_HEADER_SIZE:', http.maxHeaderSize || 8192)
    if (http.maxHeaderSize <= 8192) {
      const nodeMajorVersion = +process.version.replace(/\..*/, '').replace(/[^\d]/, '')

      const nodeVersionMsg = nodeMajorVersion < 12
        ? ' please upgrade the Node version to at least v12 and'
        : ''

      console.warn(`WARNING: The default maxHeaderSize is insufficient for some client API requests.
         Please${nodeVersionMsg} restart the server like so:

           node --max-http-header-size=50000 index.js
    `)
    }

    setupWorkerProcesses();
    console.log(`Node version: ${process.version}`);
    console.log(`listening on PORT ${AVAIL_FALCOR_PORT}`);
    console.log('AUTH_URI:', AUTH_URI);
    console.log('AUTH_PROJECT_NAME:', AUTH_PROJECT_NAME);
  } else {
    START_CONFLATION_GRAPH && loadConflationRoutingData();
    // to setup server configurations and share port address for incoming requests
    setUpExpress();
  }
};

setupServer(true);
