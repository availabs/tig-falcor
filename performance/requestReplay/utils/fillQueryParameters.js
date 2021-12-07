module.exports = (text, values = []) =>
  values
    .slice()
    .reverse()
    .reduce((query, value, i) => {
      const re = new RegExp(`\\$${values.length - i}`, "g");

      let v;

      if (typeof value === "string") {
        v = `'${value}'`;
      } else if (typeof value === "object") {
        v = `'${JSON.stringify(value).replace(/'/g, "''")}'`;
      } else if (typeof value === "number") {
        v = value;
      }

      return query.replace(re, v);
    }, text);
