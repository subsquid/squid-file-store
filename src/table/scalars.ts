import assert from 'assert'
import {BigDecimal} from '@subsquid/big-decimal'
import {Type} from './utils'

export let StringType = new Type<string>({
    dbType: 'STRING',
    serialize(value: string) {
        return value
    },
})

export let IntType = new Type<number>({
    dbType: 'INT',
    serialize(value: number) {
        assert(Number.isInteger(value), 'Invalid int')
        return value
    },
})

export let FloatType = new Type<number>({
    dbType: 'REAL',
    serialize(value: number) {
        assert(typeof value === 'number', 'Invalid float')
        return value
    },
})

export let BigIntType = new Type<bigint, string>({
    dbType: 'BIGINT',
    serialize(value: bigint) {
        assert(typeof value === 'bigint', 'Invalid bigint')
        return value.toString()
    },
})

export let BigDecimalType = new Type<BigDecimal, string>({
    dbType: 'DOUBLE',
    serialize(value: BigDecimal) {
        assert(value instanceof BigDecimal, 'Invalid bigdecimal')
        return value.toString()
    },
})

export let BooleanType = new Type<boolean>({
    dbType: 'BOOLEAN',
    serialize(value: boolean) {
        assert(typeof value === 'boolean', 'Invalid boolean')
        return value
    },
})

export let BytesType = new Type<Uint8Array, ArrayBuffer>({
    dbType: 'BLOB',
    serialize(value: Uint8Array) {
        assert(value instanceof Uint8Array, 'Invalid bytes array')
        return value.buffer
    },
})

export let DateTimeType = new Type<Date, string>({
    dbType: 'TIMESTAMPTZ',
    serialize(value: Date) {
        return value.toISOString()
    },
})
