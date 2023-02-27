# Node.js Client for Treasure Data

 [![npm version](https://badge.fury.io/js/td.svg)](https://badge.fury.io/js/td) [![Dependency Status](https://img.shields.io/librariesio/release/npm/td)](https://www.npmjs.com/package/td) [![CircleCI](https://dl.circleci.com/status-badge/img/gh/treasure-data/td-client-node/tree/master.svg?style=svg&circle-token=6f9ab6445b5cdebece6cd4c01a722677e01c039a)](https://dl.circleci.com/status-badge/redirect/gh/treasure-data/td-client-node/tree/master) [![Coverage Status](https://coveralls.io/repos/github/treasure-data/td-client-node/badge.svg?branch=master)](https://coveralls.io/github/treasure-data/td-client-node?branch=integrate-coveralls)

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, analyzing these logs easily and
reliably is a challenging task.

Treasure Data Cloud solves the problem by having: easy installation, small
footprint, plugins reliable buffering, log forwarding, the log analyzing, etc.

  * Treasure Data website: [https://www.treasuredata.com/](https://www.treasuredata.com/)
  * Treasure Data GitHub: [https://github.com/treasure-data/](https://github.com/treasure-data/)

**td-client-node** is a node.js client.

## How to Run

```js
// Client class is exposed with the name TDClient
var TDClient = require('td').TDClient;
var client = new TDClient('TREASURE_DATA_API_KEY');

var fnPrint = function(err, results) {
  console.log(results);
};

client.listDatabase(function(err, results) {
  for (var i = 0; i < results.databases.length; i++) {
    client.listTables(results.databases[i].name, fnPrint);
  }
});
```

## Requirements

node.js >= 4.8.4

## Install

    npm install td

## Generate JSDoc site

    make site

## Test

    make test

## Examples
Please see: [https://docs.treasuredata.com/articles/rest-api-node-client](https://docs.treasuredata.com/articles/rest-api-node-client)

More detail in [API reference](http://treasure-data.github.io/td-client-node/docs/index.html).

## License

Apache Software License, Version 2.0

See [LICENSE file](https://github.com/treasure-data/td-client-node/blob/master/LICENSE).
