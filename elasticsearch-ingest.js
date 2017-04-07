'use strict'

const fs = require('fs')
const path = require('path')
const H = require('highland')

const elasticsearch = require('spacetime-db-elasticsearch')

const DATASET = 'spacetime-graph'
const STEP = 'aggregate'

function ingest (config, dirs, tools, callback) {
  const dir = dirs.getDir(DATASET, STEP)
  const dataset = require(path.join(dir, `${DATASET}.dataset.json`))

  const datasetStream = H.of({
    type: 'dataset',
    action: 'create',
    payload: dataset
  })

  const objectStream = H(fs.createReadStream(path.join(dir, `${DATASET}.objects.ndjson`)))
    .split()
    .compact()
    .map(JSON.parse)
    .map((object) => ({
      type: 'object',
      meta: {
        dataset: DATASET
      },
      action: 'create',
      payload: object
    }))

  H([datasetStream, objectStream])
    .flatten()
    .batchWithTimeOrCount(1000, 500)
    .map(H.curry(elasticsearch.bulk))
    .nfcall([])
    .series()
    .errors(callback)
    .done(callback)
}

// ==================================== API ====================================

module.exports.steps = [
  ingest
]
