const AVAILRouter = require('../routes');

const { end } = require('../db_service');

const parseArgs = {
  jsonGraph: true,
  callPath: true,
  args: true,
  refPaths: true,
  thisPaths: true,
  paths: true
};

function requestToContext(queryMap) {
  const context = {};
  if (queryMap) {
    Object.keys(queryMap).forEach(key => {
      let arg = queryMap[key];
      if (parseArgs[key] && arg && typeof arg === 'string') {
        arg = arg.replace(/'/g, '"');
        arg = decodeURI(arg)
          .replace(/%2C/g, ',')
          .replace(/%3A/g, ':');
        context[key] = JSON.parse(arg);
      } else {
        context[key] = arg;
      }
    });
  }
  return context;
}

module.exports.respond = (event, cb) => {
  const promisified = typeof cb !== 'function';

  const context = requestToContext(event.queryStringParameters);

  const dataSource = AVAILRouter();

  if (Object.keys(context).length === 0) {
    const msg = `Request not supported${JSON.stringify(
      context
    )}${JSON.stringify(event)}`;
    return promisified ? Promise.reject(msg) : cb(msg, null);
  }

  if (typeof context.method === 'undefined' || context.method.length === 0) {
    const msg = 'No query method provided';
    return promisified ? Promise.reject(msg) : cb(msg, null);
  }

  if (typeof dataSource[context.method] === 'undefined') {
    const msg = `Data source does not implement the requested method ${context.method}`;
    return promisified ? Promise.reject(msg) : cb(msg, null);
  }

  let obs;
  if (context.method === 'set') {
    obs = dataSource[context.method](context.jsonGraph);
  } else if (context.method === 'call') {
    obs = dataSource[context.method](
      context.callPath,
      context.args,
      context.refPaths,
      context.thisPaths
    );
  } else {
    obs = dataSource[context.method]([].concat(context.paths));
  }

  let resolve;
  let reject;
  let promise;

  if (promisified) {
    promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
  }

  obs.subscribe(
    jsonGraphEnvelope => {
      if (promisified) {
        resolve(jsonGraphEnvelope);
      } else {
        cb(null, jsonGraphEnvelope);
      }
    },
    err => {
      const msg = `subscribe error${err}`;
      if (promisified) {
        reject(msg);
      } else {
        cb(msg, null);
      }
    }
  );

  return promise;
};

module.exports.close = () => {
  end();
};
