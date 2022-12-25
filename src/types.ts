import {BigDecimal} from '@subsquid/big-decimal'
import {assertNotNull} from '@subsquid/util-internal'
import assert from 'assert'

export class Type<T, R = T> {
    readonly dbType: string
    readonly serialize: (value: T) => R

    constructor(options: {dbType: string; serialize: (value: T) => R}) {
        this.dbType = options.dbType
        this.serialize = options.serialize
    }
}

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

class StructType<T extends Record<string, any>> extends Type<T, string> {
    readonly isStruct = true

    constructor(structType: {
        [k in keyof T]: Type<T[k], any>
    }) {
        let fields: [string, Type<any>][] = Object.entries(structType)
        super({
            dbType: `STRUCT(${fields.map(([name, type]) => `"${name}" ${type.dbType}`)})`,
            serialize(struct: T) {
                let res: string[] = []
                for (let [name, type] of fields) {
                    let value = struct[name]
                    res.push(`'${name}': '${value == null ? null : type.serialize(value)}'`)
                }
                return `{${res.join(`, `)}}`
            },
        })
    }
}

export let Struct = <T extends Record<string, any>>(structType: {
    [k in keyof T]: Type<T[k], any>
}): Type<T, string> => {
    return new StructType(structType)
}

class ListType<T> extends Type<T[], string> {
    readonly isList = true

    constructor(itemType: Type<T, any>) {
        super({
            dbType: `${itemType.dbType}[]`,
            serialize(value: T[]) {
                return `[${value.map((i) => (i == null ? null : itemType.serialize(i))).join(`, `)}]`
            },
        })
    }
}

export let List = <T>(itemType: Type<T, any>): Type<T[], string> => {
    return new ListType(itemType)
}

class NotNullType<T, R> extends Type<T, R> {
    readonly isNotNull = true

    constructor(type: Type<T, R>) {
        super({
            dbType: `${type.dbType} NOT NULL`,
            serialize(value: T) {
                return assertNotNull(type.serialize(value))
            },
        })
    }
}

export let NotNull = <T>(type: Type<T, any>): NotNullType<T, any> => {
    return new NotNullType(type)
}

type NonNullableFields<T extends Record<string, Type<any, any>>> = {
    [k in keyof T]: T[k] extends NotNullType<any, any> ? k : never
}[keyof T]

export type ConvertFieldsToTypes<T extends Record<string, Type<any>>> = {
    [k in keyof Omit<T, NonNullableFields<T>>]?: T[k] extends Type<infer R, any> ? R | null | undefined : never
} & {
    [k in keyof Pick<T, NonNullableFields<T>>]: T[k] extends Type<infer R, any> ? R : never
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

let a = {
    block: Struct({
        height: types.int,
        timestamp: types.datetime,
    }),
    extrinsicHash: types.string,
    from: types.string,
    to: types.string,
    amount: NotNull(types.bigint),
}
type A = NonNullableFields<typeof a>
