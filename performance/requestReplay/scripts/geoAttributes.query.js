#!/usr/bin/env node

const { readFileSync, writeFileSync, mkdirSync } = require("fs");
const { join } = require("path");

const fillQueryParameters = require("../utils/fillQueryParameters");

const sqlDir = join(__dirname, "../sql");

const logPath = join(__dirname, "../logs", "geoAttributes.ndjson");
const sqlPath = join(sqlDir, "geoAttributes.sql");

const query = JSON.parse(readFileSync(logPath, { encoding: "utf8" }));

mkdirSync(sqlDir, { recursive: true });

writeFileSync(sqlPath, fillQueryParameters(query.text, query.values));
