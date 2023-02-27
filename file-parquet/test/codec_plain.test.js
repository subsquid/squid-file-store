'use strict'
const assert = require('assert')
const parquet_codec_plain = require('../lib/parquet/codec/plain')

describe('ParquetCodec::PLAIN', function () {
    it('should encode BOOLEAN values', function () {
        let buf = parquet_codec_plain.encodeValues('BOOLEAN', [true, false, true, true, false, true, false, false])

        assert.deepEqual(buf, Buffer.from([0x2d])) // b101101
    })

    it('should decode BOOLEAN values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([0x2d]), // b101101
        }

        let vals = parquet_codec_plain.decodeValues('BOOLEAN', buf, 8, {})
        assert.equal(buf.offset, 1)
        assert.deepEqual(vals, [true, false, true, true, false, true, false, false])
    })

    it('should encode INT32 values', function () {
        let buf = parquet_codec_plain.encodeValues('INT32', [42, 17, 23, -1, -2, -3, 9000, 420])

        assert.deepEqual(
            buf,
            Buffer.from([
                0x2a,
                0x00,
                0x00,
                0x00, // 42
                0x11,
                0x00,
                0x00,
                0x00, // 17
                0x17,
                0x00,
                0x00,
                0x00, // 23
                0xff,
                0xff,
                0xff,
                0xff, // -1
                0xfe,
                0xff,
                0xff,
                0xff, // -2
                0xfd,
                0xff,
                0xff,
                0xff, // -3
                0x28,
                0x23,
                0x00,
                0x00, // 9000
                0xa4,
                0x01,
                0x00,
                0x00, // 420
            ])
        )
    })

    it('should decode INT32 values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([
                0x2a,
                0x00,
                0x00,
                0x00, // 42
                0x11,
                0x00,
                0x00,
                0x00, // 17
                0x17,
                0x00,
                0x00,
                0x00, // 23
                0xff,
                0xff,
                0xff,
                0xff, // -1
                0xfe,
                0xff,
                0xff,
                0xff, // -2
                0xfd,
                0xff,
                0xff,
                0xff, // -3
                0x28,
                0x23,
                0x00,
                0x00, // 9000
                0xa4,
                0x01,
                0x00,
                0x00, // 420
            ]),
        }

        let vals = parquet_codec_plain.decodeValues('INT32', buf, 8, {})
        assert.equal(buf.offset, 32)
        assert.deepEqual(vals, [42, 17, 23, -1, -2, -3, 9000, 420])
    })

    it('should encode INT64 values', function () {
        let buf = parquet_codec_plain.encodeValues('INT64', [42n, 17n, 23n, -1n, -2n, -3n, 9000n, 420n])

        assert.deepEqual(
            buf,
            Buffer.from([
                0x2a,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 42
                0x11,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 17
                0x17,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 23
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -1
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -2
                0xfd,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -3
                0x28,
                0x23,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 9000
                0xa4,
                0x01,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 420
            ])
        )
    })

    it('should decode INT64 values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([
                0x2a,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 42
                0x11,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 17
                0x17,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 23
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -1
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -2
                0xfd,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -3
                0x28,
                0x23,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 9000
                0xa4,
                0x01,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 420
            ]),
        }

        let vals = parquet_codec_plain.decodeValues('INT64', buf, 8, {})
        assert.equal(buf.offset, 64)
        assert.deepEqual(vals, [42n, 17n, 23n, -1n, -2n, -3n, 9000n, 420n])
    })

    it('should encode INT96 values', function () {
        let buf = parquet_codec_plain.encodeValues('INT96', [42, 17, 23, -1, -2, -3, 9000, 420])

        assert.deepEqual(
            buf,
            Buffer.from([
                0x2a,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 42
                0x11,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 17
                0x17,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 23
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -1
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -2
                0xfd,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -3
                0x28,
                0x23,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 9000
                0xa4,
                0x01,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 420
            ])
        )
    })

    it('should decode INT96 values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([
                0x2a,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 42
                0x11,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 17
                0x17,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 23
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -1
                0xfe,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -2
                0xfd,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff,
                0xff, // -3
                0x28,
                0x23,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 9000
                0xa4,
                0x01,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00, // 420
            ]),
        }

        let vals = parquet_codec_plain.decodeValues('INT96', buf, 8, {})
        assert.equal(buf.offset, 96)
        assert.deepEqual(vals, [42, 17, 23, -1, -2, -3, 9000, 420])
    })

    it('should encode FLOAT values', function () {
        let buf = parquet_codec_plain.encodeValues('FLOAT', [42.0, 23.5, 17.0, 4.2, 9000])

        assert.deepEqual(
            buf,
            Buffer.from([
                0x00,
                0x00,
                0x28,
                0x42, // 42.0
                0x00,
                0x00,
                0xbc,
                0x41, // 23.5
                0x00,
                0x00,
                0x88,
                0x41, // 17.0
                0x66,
                0x66,
                0x86,
                0x40, // 4.20
                0x00,
                0xa0,
                0x0c,
                0x46, // 9000
            ])
        )
    })

    it('should decode FLOAT values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([
                0x00,
                0x00,
                0x28,
                0x42, // 42.0
                0x00,
                0x00,
                0xbc,
                0x41, // 23.5
                0x00,
                0x00,
                0x88,
                0x41, // 17.0
                0x66,
                0x66,
                0x86,
                0x40, // 4.20
                0x00,
                0xa0,
                0x0c,
                0x46, // 9000
            ]),
        }

        let vals = parquet_codec_plain.decodeValues('FLOAT', buf, 5, {})
        assert.equal(buf.offset, 20)
        assert_util.assertArrayEqualEpsilon(vals, [42.0, 23.5, 17.0, 4.2, 9000])
    })

    it('should encode DOUBLE values', function () {
        let buf = parquet_codec_plain.encodeValues('DOUBLE', [42.0, 23.5, 17.0, 4.2, 9000])

        assert.deepEqual(
            buf,
            Buffer.from([
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x45,
                0x40, // 42.0
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x80,
                0x37,
                0x40, // 23.5
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x31,
                0x40, // 17.0
                0xcd,
                0xcc,
                0xcc,
                0xcc,
                0xcc,
                0xcc,
                0x10,
                0x40, // 4.20
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x94,
                0xc1,
                0x40, // 9000
            ])
        )
    })

    it('should decode DOUBLE values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x45,
                0x40, // 42.0
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x80,
                0x37,
                0x40, // 23.5
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x31,
                0x40, // 17.0
                0xcd,
                0xcc,
                0xcc,
                0xcc,
                0xcc,
                0xcc,
                0x10,
                0x40, // 4.20
                0x00,
                0x00,
                0x00,
                0x00,
                0x00,
                0x94,
                0xc1,
                0x40, // 9000
            ]),
        }

        let vals = parquet_codec_plain.decodeValues('DOUBLE', buf, 5, {})
        assert.equal(buf.offset, 40)
        assert_util.assertArrayEqualEpsilon(vals, [42.0, 23.5, 17.0, 4.2, 9000])
    })

    it('should encode BYTE_ARRAY values', function () {
        let buf = parquet_codec_plain.encodeValues('BYTE_ARRAY', [
            'one',
            Buffer.from([0xde, 0xad, 0xbe, 0xef]),
            'three',
        ])

        assert.deepEqual(
            buf,
            Buffer.from([
                0x03,
                0x00,
                0x00,
                0x00, // (3)
                0x6f,
                0x6e,
                0x65, // 'one'
                0x04,
                0x00,
                0x00,
                0x00, // (4)
                0xde,
                0xad,
                0xbe,
                0xef, // 0xdeadbeef
                0x05,
                0x00,
                0x00,
                0x00, // (5)
                0x74,
                0x68,
                0x72,
                0x65,
                0x65, // 'three'
            ])
        )
    })

    it('should decode BYTE_ARRAY values', function () {
        let buf = {
            offset: 0,
            buffer: Buffer.from([
                0x03,
                0x00,
                0x00,
                0x00, // (3)
                0x6f,
                0x6e,
                0x65, // 'one'
                0x04,
                0x00,
                0x00,
                0x00, // (4)
                0xde,
                0xad,
                0xbe,
                0xef, // 0xdeadbeef
                0x05,
                0x00,
                0x00,
                0x00, // (5)
                0x74,
                0x68,
                0x72,
                0x65,
                0x65, // 'three'
            ]),
        }

        let vals = parquet_codec_plain.decodeValues('BYTE_ARRAY', buf, 3, {})
        assert.equal(buf.offset, 24)
        assert.deepEqual(vals, [Buffer.from('one'), Buffer.from([0xde, 0xad, 0xbe, 0xef]), Buffer.from('three')])
    })

    it('should encode FIXED_LEN_BYTE_ARRAY values', function () {
        let buf = parquet_codec_plain.encodeValues(
            'FIXED_LEN_BYTE_ARRAY',
            ['oneoo', Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x42]), 'three'],
            {
                typeLength: 5,
            }
        )

        assert.deepEqual(
            buf,
            Buffer.from([
                0x6f,
                0x6e,
                0x65,
                0x6f,
                0x6f, // 'oneoo'
                0xde,
                0xad,
                0xbe,
                0xef,
                0x42, // 0xdeadbeef42
                0x74,
                0x68,
                0x72,
                0x65,
                0x65, // 'three'
            ])
        )
    })
})
