import {Int16, Int32, Int64, Int8, Uint16, Uint32, Uint64, Uint8, Utf8, DecimalBuilder} from 'apache-arrow'
import assert from 'assert'
import {ParquetType} from './type'

export let StringType = (): ParquetType<string> => ({
    getArrowDataType() {
        return new Utf8()
    },
    validate(value) {
        assert(typeof value === 'string')
        return value
    },
})

export let Int8Type = (): ParquetType<number> => ({
    getArrowDataType() {
        return new Int8()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let Int16Type = (): ParquetType<number> => ({
    getArrowDataType() {
        return new Int16()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let Int32Type = (): ParquetType<number> => ({
    getArrowDataType() {
        return new Int32()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let Int64Type = (): ParquetType<bigint> => ({
    getArrowDataType() {
        return new Int64()
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let UInt8Type = (): ParquetType<number> => ({
    getArrowDataType() {
        return new Uint8()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let UInt16Type = (): ParquetType<number> => ({
    getArrowDataType() {
        return new Uint16()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let UInt32Type = (): ParquetType<bigint> => ({
    getArrowDataType() {
        return new Uint32()
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let UInt64Type = (): ParquetType<bigint> => ({
    getArrowDataType() {
        return new Uint64()
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})
