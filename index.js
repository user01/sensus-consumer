#!/usr/bin/env node

const argv = require('yargs').argv;
const glob = require("glob");
const path = require('path');
const R = require('ramda');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require("fs"));
const env = require('dotenv').config();
const Pool = require('pg').Pool;

const cwd = argv.cwd || argv._[0];


const pool = new Pool({
  host: env.db_host,
  user: env.db_user,
  password: env.db_password,
  database: env.db_database,
  port: 5432, //env var: PGPORT
  max: 10, // max number of clients in the pool
  idleTimeoutMillis: 30000, // how long a client is allowed to remain idle before being closed
});

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

// Generate the chain of value sets
const values_chain = R.pipe(
  R.length,
  // R.add(-1),
  R.range(0),
  R.map(i => `($${i+1})`),
  R.join(', ')
);

// Handle json object from file
// Fix type, dates, etc
const handle_json = (full_json) => {
  if (full_json.length < 1) return [];
  const type = {
    '$type': get_type(full_json)
  };
  return R.pipe(
    R.map(R.merge(R.__, type)),
    R.slice(0, 3)
  )(full_json);
};

const write_json = (updated_json) => {
  return new Promise(function(resolve, reject) {
    if (updated_json.length < 1) return resolve(0);

    pool.query(`INSERT INTO sensus (datum) VALUES ${values_chain(updated_json)}`, updated_json, (err) => {
      if (err) {
        console.error(`\n\nDatabase write failed\n\n`);
        console.log(err);
      } else {
        process.stdout.write(".");
      }
      resolve(updated_json.length);
    });
  });
}

// Do everything for a single path
const handle_json_path = (fullpath) => {
  return fs.readFileAsync(fullpath, "utf8")
    .then((buf) => {
      return buf.length < 3 ? [] : JSON.parse(buf.toString());
    })
    .then(handle_json)
    .then(write_json);
}


pool
  .query(`CREATE TABLE IF NOT EXISTS sensus ( id serial primary key, datum jsonb );
          CREATE INDEX ON sensus((datum->>'$type'));
          CREATE INDEX ON sensus((datum->>'DeviceId'));`)
  .then(() => {
    return get_json_paths(cwd)
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
        console.log(R.sum(res));
        return R.sum(res);
      });
  })
  .then((final_count) => {
    console.log(`Written ${final_count} records.`);
    return final_count;
  });
