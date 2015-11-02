# Node.js Client for Treasure Data

[![Circle CI](https://circleci.com/gh/treasure-data/td-client-node.svg?style=svg&circle-token=3e6d45d70e790212c0aa5a974f3daf8656fd3a37)](https://circleci.com/gh/treasure-data/td-client-node)

## Overview

Many web/mobile applications generate huge amount of event logs (c,f. login,
logout, purchase, follow, etc).  Analyzing these event logs can be quite
valuable for improving services.  However, analyzing these logs easily and 
reliably is a challenging task.

Treasure Data Cloud solves the problem by having: easy installation, small 
footprint, plugins reliable buffering, log forwarding, the log analyzing, etc.

  * Treasure Data website: [http://treasure-data.com/](http://treasure-data.com/)
  * Treasure Data GitHub: [https://github.com/treasure-data/](https://github.com/treasure-data/)

**td-client-node** is a node.js client.

## Requirements

node.js >= 0.8

## Install

    npm install td

## Generate JSDoc site

    make site

## Examples
Please see: [http://docs.treasure-data.com/articles/rest-api-node-client](http://docs.treasure-data.com/articles/rest-api-node-client)

## License

Apache Software License, Version 2.0

See [LICENSE file](https://github.com/treasure-data/td-client-node/blob/master/LICENSE).
