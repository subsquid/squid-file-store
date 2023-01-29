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
import {Type} from './type'

export let StringType = (): Type<string> => ({
    arrowDataType: new Utf8(),
    prepare(value) {
        return value
    },
})

export let Int8Type = (): Type<number> => ({
    arrowDataType: new Int8(),
    prepare(value) {
        return value
    },
})

export let Int16Type = (): Type<number> => ({
    arrowDataType: new Int16(),
    prepare(value) {
        return value
    },
})

export let Int32Type = (): Type<number> => ({
    arrowDataType: new Int32(),
    prepare(value) {
        return value
    },
})

export let Int64Type = (): Type<bigint> => ({
    arrowDataType: new Int64(),
    prepare(value) {
        return value
    },
})

export let Uint8Type = (): Type<number> => ({
    arrowDataType: new Uint8(),
    prepare(value) {
        return value
    },
})

export let Uint16Type = (): Type<number> => ({
    arrowDataType: new Uint16(),
    prepare(value) {
        return value
    },
})

export let Uint32Type = (): Type<bigint> => ({
    arrowDataType: new Uint32(),
    prepare(value) {
        return value
    },
})

export let Uint64Type = (): Type<bigint> => ({
    arrowDataType: new Uint64(),
    prepare(value) {
        return value
    },
})

export let FloatType = (): Type<number> => ({
    arrowDataType: new Float32(),
    prepare(value) {
        return value
    },
})

export let BooleanType = (): Type<boolean> => ({
    arrowDataType: new Bool(),
    prepare(value) {
        return value
    },
})

export let TimestampType = (): Type<Date> => ({
    arrowDataType: new Timestamp(TimeUnit.MILLISECOND),
    prepare(value) {
        return value.valueOf()
    },
})

export let DateType = (): Type<Date> => ({
    arrowDataType: new Date_(DateUnit.DAY),
    prepare(value) {
        return value
    },
})
