const http = require("http"),
  https = require("https"),

  regex = /^https:\/\//;

const DEFAULT_OPTIONS = {
  method: "GET",
  body: null
}

module.exports = (url, options = DEFAULT_OPTIONS) => {
  const SERVICE = regex.test(url) ? https : http;

  const {
    body,
    ...rest
  } = options

  return new Promise((resolve, reject) => {

    const request = SERVICE.request(url, rest, res => {
      const { statusCode } = res;
      //console.log('test 123', res)
      if (statusCode !== 200) {
        return reject(new Error(`URL ${ url } failed, with: ${ statusCode }`));
      }

      res.on('error', reject);

      const chunks = [];
      res.on('data', chunk => {
        chunks.push(chunk);
      });
      res.on('end', () => {
        try {
          resolve(JSON.parse(chunks.join("")));
        }
        catch (e) {
          reject(e);
        }
      });
    }) // END SERVICE.request

    request.on('error', reject);
    body && request.write(body);
    request.end();
  })
}
