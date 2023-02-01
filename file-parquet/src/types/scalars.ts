import * as Arrow from 'apache-arrow'
import {Type} from '../table'

export let String = (): Type<string> => ({
    arrowDataType: new Arrow.Utf8(),
    prepare(value) {
        return value
    },
})

export let Int8 = (): Type<number> => ({
    arrowDataType: new Arrow.Int8(),
    prepare(value) {
        return value
    },
})

export let Int16 = (): Type<number> => ({
    arrowDataType: new Arrow.Int16(),
    prepare(value) {
        return value
    },
})

export let Int32 = (): Type<number> => ({
    arrowDataType: new Arrow.Int32(),
    prepare(value) {
        return value
    },
})

export let Int64 = (): Type<bigint> => ({
    arrowDataType: new Arrow.Int64(),
    prepare(value) {
        return value
    },
})

export let Uint8 = (): Type<number> => ({
    arrowDataType: new Arrow.Uint8(),
    prepare(value) {
        return value
    },
})

export let Uint16 = (): Type<number> => ({
    arrowDataType: new Arrow.Uint16(),
    prepare(value) {
        return value
    },
})

export let Uint32 = (): Type<number> => ({
    arrowDataType: new Arrow.Uint32(),
    prepare(value) {
        return value
    },
})

export let Uint64 = (): Type<bigint> => ({
    arrowDataType: new Arrow.Uint64(),
    prepare(value) {
        return value
    },
})

export let Float = (): Type<number> => ({
    arrowDataType: new Arrow.Float32(),
    prepare(value) {
        return value
    },
})

export let Boolean = (): Type<boolean> => ({
    arrowDataType: new Arrow.Bool(),
    prepare(value) {
        return value
    },
})

export let Timestamp = (): Type<Date> => ({
    arrowDataType: new Arrow.Timestamp(Arrow.TimeUnit.MILLISECOND),
    prepare(value) {
        return value.valueOf()
    },
})

export let Date = (): Type<Date> => ({
    arrowDataType: new Arrow.Date_(Arrow.DateUnit.DAY),
    prepare(value) {
        return value
    },
})
