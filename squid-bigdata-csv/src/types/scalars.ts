import assert from 'assert'
import {CsvType} from './type'

export let StringType = (): CsvType<string> => ({
    serialize(value) {
        return value
    },
    validate(value) {
        assert(typeof value === 'string')
        return value
    },
})

export let NumberType = (): CsvType<number> => ({
    serialize(value: number) {
        return value.toString()
    },
    validate(value) {
        assert(typeof value === 'number')
        return value
    },
})

export let BigIntType = (): CsvType<bigint> => ({
    serialize(value: bigint) {
        return value.toString()
    },
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let BooleanType = (): CsvType<boolean> => ({
    serialize(value: boolean) {
        assert(typeof value === 'boolean', 'Invalid boolean')
        return value.toString()
    },
    validate(value) {
        assert(typeof value === 'boolean')
        return value
    },
})

export let TimestampType = (): CsvType<Date> => ({
    serialize(value: Date) {
        return value.toISOString()
    },
    validate(value) {
        assert(value instanceof Date)
        return value
    },
})
