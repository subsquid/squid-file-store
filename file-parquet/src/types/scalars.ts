import {Type} from '../table'

export let String = (): Type<string> => ({
    parquetType: 'UTF8',
})

export let Int8 = (): Type<number> => ({
    parquetType: 'INT_8',
})

export let Int16 = (): Type<number> => ({
    parquetType: 'INT_16',
})

export let Int32 = (): Type<number> => ({
    parquetType: 'INT_32',
})

export let Int64 = (): Type<bigint> => ({
    parquetType: 'INT_64',
})

export let Uint8 = (): Type<number> => ({
    parquetType: 'UINT_8',
})

export let Uint16 = (): Type<number> => ({
    parquetType: 'UINT_16',
})

export let Uint32 = (): Type<number> => ({
    parquetType: 'UINT_32',
})

export let Uint64 = (): Type<bigint> => ({
    parquetType: 'UINT_64',
})

export let Float = (): Type<number> => ({
    parquetType: 'FLOAT',
})

export let Boolean = (): Type<boolean> => ({
    parquetType: 'BOOLEAN',
})

export let Timestamp = (): Type<Date> => ({
    parquetType: 'TIMESTAMP_MILLIS',
})
