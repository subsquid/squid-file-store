import {
    Int16,
    Int32,
    Int64,
    Int8,
    Uint16,
    Uint32,
    Uint64,
    Uint8,
    Utf8,
    Date_,
    Timestamp,
    DateUnit,
    TimeUnit,
    Float32,
    Bool,
} from 'apache-arrow'
import assert from 'assert'
import {ParquetType} from './type'

export let StringType = (): ParquetType<string> => ({
    arrowDataType: new Utf8(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'string')
        return value
    },
})

export let Int8Type = (): ParquetType<number> => ({
    arrowDataType: new Int8(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Int16Type = (): ParquetType<number> => ({
    arrowDataType: new Int16(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Int32Type = (): ParquetType<number> => ({
    arrowDataType: new Int32(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Int64Type = (): ParquetType<bigint> => ({
    arrowDataType: new Int64(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let Uint8Type = (): ParquetType<number> => ({
    arrowDataType: new Uint8(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Uint16Type = (): ParquetType<number> => ({
    arrowDataType: new Uint16(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Uint32Type = (): ParquetType<bigint> => ({
    arrowDataType: new Uint32(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let Uint64Type = (): ParquetType<bigint> => ({
    arrowDataType: new Uint64(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let FloatType = (): ParquetType<number> => ({
    arrowDataType: new Float32(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let BooleanType = (): ParquetType<boolean> => ({
    arrowDataType: new Bool(),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'boolean')
        return value
    },
})

export let TimestampType = (): ParquetType<Date> => ({
    arrowDataType: new Timestamp(TimeUnit.MILLISECOND),
    prepare(value) {
        return this.validate(value).valueOf()
    },
    validate(value) {
        assert(value instanceof Date)
        return value
    },
})

export let DateType = (): ParquetType<Date> => ({
    arrowDataType: new Date_(DateUnit.DAY),
    prepare(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(value instanceof Date)
        return value
    },
})
