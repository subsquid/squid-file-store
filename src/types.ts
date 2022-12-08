import {BigDecimal} from '@subsquid/big-decimal'
import {toHex} from '@subsquid/util-internal-hex'
import {toJSON} from '@subsquid/util-internal-json'
import assert from 'assert'
import {Dialect, Quote} from './dialect'

export class Type<T> {
    readonly name: string
    readonly serialize: (value: T, dialect: Dialect) => string

    constructor(options: {name: string; serialize: (value: T, dialect: Dialect) => string}) {
        this.name = options.name
        this.serialize = options.serialize
    }
}

export let StringType = new Type<string>({
    name: 'string',
    serialize(value: string, dialect: Dialect) {
        return quoteString(value, dialect)
    },
})

export let IntType = new Type<number>({
    name: 'int',
    serialize(value: number, dialect: Dialect) {
        assert(Number.isInteger(value), 'Invalid int')
        return quoteString(value.toString(), dialect, true)
    },
})

export let FloatType = new Type<number>({
    name: 'float',
    serialize(value: number, dialect: Dialect) {
        assert(typeof value === 'number', 'Invalid float')
        return quoteString(value.toString(), dialect, true)
    },
})

export let BigIntType = new Type<bigint>({
    name: 'bigint',
    serialize(value: bigint, dialect: Dialect) {
        assert(typeof value === 'bigint', 'Invalid bigint')
        return quoteString(value.toString(), dialect, true)
    },
})

export let BigDecimalType = new Type<BigDecimal>({
    name: 'bigdecimal',
    serialize(value: BigDecimal, dialect: Dialect) {
        assert(value instanceof BigDecimal, 'Invalid bigdecimal')
        return quoteString(value.toString(), dialect, true)
    },
})

export let BooleanType = new Type<boolean>({
    name: 'boolean',
    serialize(value: boolean, dialect: Dialect) {
        assert(typeof value === 'boolean', 'Invalid boolean')
        return quoteString(value.toString(), dialect)
    },
})

export let BytesType = new Type<Uint8Array>({
    name: 'bytes',
    serialize(value: Uint8Array, dialect: Dialect) {
        assert(value instanceof Uint8Array, 'Invalid bytes array')
        return quoteString(toHex(value), dialect)
    },
})

export let DateTimeType = new Type<Date>({
    name: 'datetime',
    serialize(value: Date, dialect: Dialect) {
        return quoteString(value.toISOString(), dialect)
    },
})

export let JSONType = new Type<any>({
    name: 'json',
    serialize(value: any, dialect: Dialect) {
        return quoteString(JSON.stringify(toJSON(value)), dialect)
    },
})

export let ArrayType = <T>(itemType: Type<T>): Type<T[]> => {
    return new Type({
        name: `array<${itemType.name}>`,
        serialize(value: T[], dialect: Dialect) {
            return value.map((i) => itemType.serialize(i, dialect)).join('|')
        },
    })
}

export let Nullable = <T>(type: Type<T>): Type<T | null | undefined> => {
    return new Type({
        name: `nullable<${type.name}>`,
        serialize(value: T | null | undefined, dialect: Dialect) {
            return value == null ? '' : type.serialize(value, dialect)
        },
    })
}

export let types = {
    string: StringType,
    int: IntType,
    float: FloatType,
    bigint: BigIntType,
    bigdecimal: BigDecimalType,
    bytes: BytesType,
    datetime: DateTimeType,
    boolean: BooleanType,
}

function hasSpecialChar(str: string, dialect: Dialect) {
    return (
        str.includes(dialect.delimiter) ||
        str.includes(dialect.arrayDelimiter) ||
        str.includes(dialect.lineTerminator) ||
        str.includes(dialect.quoteChar)
    )
}

function quoteString(str: string, dialect: Dialect, numeric = false) {
    switch (dialect.quoting) {
        case Quote.ALL:
        case Quote.NONNUMERIC:
            if (!numeric) return `${dialect.quoteChar}${str}${dialect.quoteChar}`
        case Quote.MINIMAL:
            if (hasSpecialChar(str, dialect)) {
                return `${dialect.quoteChar}${str}${dialect.quoteChar}`
            } else {
                return str
            }
        case Quote.NONE:
            return str
        default:
            throw new UnexpectedQuoting(dialect.quoting)
    }
}

class UnexpectedQuoting extends Error {
    constructor(value: Quote) {
        super(`Unexpected Quoting case: ${value}`)
    }
}
