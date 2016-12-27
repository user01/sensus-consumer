# Testing ingestion of Sensus JSON

This is a short script designed to ingest the flat sensus json files into a postgres database.

After `npm install`, the code can be run with `node node index.js ./some/path/to/sensus/data/parent/directory`.

The code assumes postgres server connection details are placed in a `.env` text file of the form:

```
db_host=localhost
db_user=sensususer
db_database=sensusdb
db_password=somepassword
```
