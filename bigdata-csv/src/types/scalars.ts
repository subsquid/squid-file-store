import assert from 'assert'
import {CsvType} from './type'

export let StringType = (): CsvType<string> => ({
    serialize(value) {
        return this.validate(value)
    },
    validate(value) {
        assert(typeof value === 'string')
        return value
    },
})

export let NumberType = (): CsvType<number> => ({
    serialize(value: number) {
        return this.validate(value).toString()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let BigIntType = (): CsvType<bigint> => ({
    serialize(value: bigint) {
        return this.validate(value).toString()
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let BooleanType = (): CsvType<boolean> => ({
    serialize(value: boolean) {
        assert(typeof value === 'boolean', 'Invalid boolean')
        return this.validate(value).toString()
    },
    validate(value) {
        assert(typeof value === 'boolean')
        return value
    },
})

export let TimestampType = (): CsvType<Date> => ({
    serialize(value: Date) {
        return this.validate(value).valueOf().toString()
    },
    validate(value) {
        assert(value instanceof Date)
        return value
    },
})

export let DateTimeType = (): CsvType<Date> => ({
    serialize(value: Date) {
        return this.validate(value).toISOString()
    },
    validate(value) {
        assert(value instanceof Date)
        return value
    },
})
