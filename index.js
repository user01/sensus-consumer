#!/usr/bin/env node

const argv = require('yargs').argv;
const glob = require("glob");
const path = require('path');
const R = require('ramda');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require("fs"));

const cwd = argv.cwd || argv._[0];

// Glob up appropriate file paths
const get_json_paths = (cwd) => {
  return new Promise(function(resolve, reject) {
    glob("**/*.json", {
      cwd
    }, (err, files) => {
      console.log(`Found ${files.length} files.`);
      resolve(files.map(file => path.join(cwd, file)));
    });
  });
};

// Pipeline to remove the namespace junk from the $type field
const get_type = R.pipe(
  R.head,
  R.prop('$type'),
  R.split(", "),
  R.head,
  R.split("."),
  R.last
);

// Handle json object from file
// Fix type, dates, etc
const handle_json = (full_json) => {
  if (full_json.length < 1) return [];
  const type = get_type(full_json);
  return full_json.map((elm) => R.merge(elm, {
    '$type': type
  }));
};

// Do everything for a single path
const handle_json_path = (fullpath) => {
  return fs.readFileAsync(fullpath, "utf8").then((buf) => {
    return buf.length < 3 ? [] : JSON.parse(buf.toString());
  }).then(handle_json);
}

get_json_paths(cwd)
  .then((res) => {
    return R.slice(0, 5, res); // TODO: Remove DEBUG limiter
  })
  .then((fullpaths) => {
    return Promise.map(fullpaths, handle_json_path, {
      concurrency: 2
    });
  })
  .then((res) => {
    console.log('Final result');
    console.log(res);
  });
