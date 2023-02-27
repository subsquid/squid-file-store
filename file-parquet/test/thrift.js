'use strict'
const assert = require('assert')
const thrift = require('thrift')
const parquet_thrift = require('../thrift/parquet_types')
const parquet_util = require('../lib/parquet/util')

describe('Thrift', function () {
    it('should correctly en/decode literal zeroes with the CompactProtocol', function () {
        let obj = new parquet_thrift.ColumnMetaData()
        obj.num_values = 0

        let obj_bin = parquet_util.serializeThrift(obj)
        assert.equal(obj_bin.length, 3)
    })
})
