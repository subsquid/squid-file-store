import assert from 'assert'
import {BigDecimal} from '@subsquid/big-decimal'
import {Type} from './utils'

export let StringType = new Type<string>({
    dbType: 'STRING',
    serialize(value: string) {
        return value
    },
})

export let TinyIntType = new Type<number>({
    dbType: 'TINYINT',
    serialize(value: number) {
        return value
    },
})

export let UTinyIntType = new Type<number>({
    dbType: 'UTINYINT',
    serialize(value: number) {
        return value
    },
})

export let SmallIntType = new Type<number>({
    dbType: 'SMALLINT',
    serialize(value: number) {
        return value
    },
})

export let USmallIntType = new Type<number>({
    dbType: 'USMALLINT',
    serialize(value: number) {
        return value
    },
})

export let IntType = new Type<number>({
    dbType: 'INT',
    serialize(value: number) {
        return value
    },
})

export let UIntType = new Type<number>({
    dbType: 'UINT',
    serialize(value: number) {
        return value
    },
})

export let BigIntType = new Type<bigint, string>({
    dbType: 'BIGINT',
    serialize(value: bigint) {
        return value.toString()
    },
})

export let UBigIntType = new Type<bigint, string>({
    dbType: 'BIGINT',
    serialize(value: bigint) {
        return value.toString()
    },
})

export let HugeIntType = new Type<bigint, string>({
    dbType: 'HIGEINT',
    serialize(value: bigint) {
        return value.toString()
    },
})

export let FloatType = new Type<number>({
    dbType: 'REAL',
    serialize(value: number) {
        return value
    },
})

export let DoubleType = new Type<number>({
    dbType: 'DOUBLE',
    serialize(value: number) {
        return value
    },
})

// export let BigDecimalType = new Type<BigDecimal, string>({
//     dbType: 'DOUBLE',
//     serialize(value: BigDecimal) {
//         assert(value instanceof BigDecimal, 'Invalid bigdecimal')
//         return value.toString()
//     },
// })

export let BooleanType = new Type<boolean>({
    dbType: 'BOOLEAN',
    serialize(value: boolean) {
        assert(typeof value === 'boolean', 'Invalid boolean')
        return value
    },
})

export let BlobType = new Type<Uint8Array, ArrayBuffer>({
    dbType: 'BLOB',
    serialize(value: Uint8Array) {
        assert(value instanceof Uint8Array, 'Invalid bytes array')
        return value.buffer
    },
})

export let DateType = new Type<Date, string>({
    dbType: 'DATE',
    serialize(value: Date) {
        return value.toISOString()
    },
})

export let TimestampType = new Type<Date, string>({
    dbType: 'TIMESTAMP',
    serialize(value: Date) {
        return value.toISOString()
    },
})

export let TimestampZType = new Type<Date, string>({
    dbType: 'TIMESTAMPZ',
    serialize(value: Date) {
        return value.toISOString()
    },
})
