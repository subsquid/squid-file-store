import assert from 'assert'
import {Type} from './utils'

export let StringType = (): Type<string> => ({
    dbType: 'string',
    serialize(value: string) {
        return `${value}`
    },
})

export let NumberType = (): Type<number> => ({
    dbType: 'number',
    serialize(value: number) {
        return value.toString()
    },
})

export let BigIntType = (): Type<bigint> => ({
    dbType: 'bigint',
    serialize(value: bigint) {
        return value.toString()
    },
})

export let BooleanType = (): Type<boolean> => ({
    dbType: 'BOOLEAN',
    serialize(value: boolean) {
        assert(typeof value === 'boolean', 'Invalid boolean')
        return value.toString()
    },
})

export let TimestampType = (): Type<Date> => ({
    dbType: 'TIMESTAMP WITH TIME ZONE',
    serialize(value: Date) {
        return value.toISOString()
    },
})
