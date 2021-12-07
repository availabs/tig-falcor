const request = require('request-promise-native');

const { AUTH_URI, AUTH_PROJECT_NAME } = require('../../server-config');

const cacheOptions = {
  stdTTL: 300 /* time-to-live in seconds */
};

const NodeCache = require('node-cache');
const authCache = new NodeCache(cacheOptions);

const authorizeJWT = async token => {
  const body = JSON.stringify({ token, project: AUTH_PROJECT_NAME });
  const options = {
    headers: {
      Accept: 'application/json, text/plain, */*',
      'Content-Type': 'application/json'
    },
    method: 'POST',
    uri: AUTH_URI,
    body
  };

  const authServerResponse = await request(options);

  const { user } = JSON.parse(authServerResponse);

  return user || null;
};

const jwtAuth = async (req, res, next) => {
  // No auth for OPTIONS requests:
  //   https://stackoverflow.com/a/40723041
  if (req.method === 'OPTIONS') {
    return next();
  }

  const { authorization: token } = req.headers;

  if (!token) {
    return next();
  }

  try {
    let user = authCache.get(token);

    if (user) {
      authCache.ttl(token);
    } else {
      user = await authorizeJWT(token);
      if (user) {
        authCache.set(token, user);
      }
    }

    req.availAuthContext = { user };

    return next();
  } catch (err) {
    console.error(err);
    return res
      .status(500)
      .send({ error: 'Error attempting JWT authorization.' });
  }
};

module.exports = jwtAuth;
