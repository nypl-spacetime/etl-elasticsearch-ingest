'use strict'

const fs = require('fs')
const path = require('path')
const H = require('highland')

const elasticsearch = require('spacetime-db-elasticsearch')

const DATASET = 'spacetime-graph'
const STEP = 'aggregate'

function addVersion (dataset, version) {
  return `${dataset}-v${version}`
}

function getVersion (index) {
  const match = /-v(\d+)$/.exec(index)
  return parseInt(match[1])
}

function createIndex (dataset, ndjsonStream, version, callback) {
  const versionedDataset = Object.assign({}, dataset, {
    id: addVersion(dataset.id, version)
  })

  const datasetStream = H.of({
    type: 'dataset',
    action: 'create',
    payload: versionedDataset
  })

  const objectStream = H(ndjsonStream)
    .split()
    .compact()
    .map(JSON.parse)
    .map((object) => ({
      type: 'object',
      meta: {
        dataset: addVersion(DATASET, version)
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
    .done(() => {
      const indexNew = addVersion(DATASET, version)

      if (version > 1) {
        const indexOld = addVersion(DATASET, version - 1)

        elasticsearch.updateAliases(indexOld, indexNew, DATASET, (err, resp) => {
          if (err) {
            callback(err)
            return
          }

          elasticsearch.delete([indexOld], callback)
        })
      } else {
        elasticsearch.putAlias(indexNew, DATASET, callback)
      }
    })
}

function ingest (config, dirs, tools, callback) {
  const dir = dirs.getDir(DATASET, STEP)
  const dataset = require(path.join(dir, `${DATASET}.dataset.json`))
  const ndjsonStream = fs.createReadStream(path.join(dir, `${DATASET}.objects.ndjson`))

  // Use alias to swap old + new index
  // https://www.elastic.co/guide/en/elasticsearch/guide/current/index-aliases.html
  elasticsearch.getAliasedIndex(DATASET, (err, index) => {
    if (err) {
      if (err.status === 404) {
        // alias does not exist, set version to 1!
        createIndex(dataset, ndjsonStream, 1, callback)
      } else {
        callback(err)
      }
    } else {
      const currentVersion = getVersion(index)
      createIndex(dataset, ndjsonStream, currentVersion + 1, callback)
    }
  })
}

// ==================================== API ====================================

module.exports.steps = [
  ingest
]
