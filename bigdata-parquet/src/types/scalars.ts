import {
    Decimal,
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
    Time,
    Interval,
    IntervalUnit,
} from 'apache-arrow'
import assert from 'assert'
import {ParquetType} from './type'

export let StringType = (): ParquetType<string> => ({
    arrowDataType: new Utf8(),
    validate(value) {
        assert(typeof value === 'string')
        return value
    },
})

export let Int8Type = (): ParquetType<number> => ({
    arrowDataType: new Int8(),
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Int16Type = (): ParquetType<number> => ({
    arrowDataType: new Int16(),
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Int32Type = (): ParquetType<number> => ({
    arrowDataType: new Int32(),
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Int64Type = (): ParquetType<bigint> => ({
    arrowDataType: new Int64(),
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let Uint8Type = (): ParquetType<number> => ({
    arrowDataType: new Uint8(),
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Uint16Type = (): ParquetType<number> => ({
    arrowDataType: new Uint16(),
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let Uint32Type = (): ParquetType<bigint> => ({
    arrowDataType: new Uint32(),
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let Uint64Type = (): ParquetType<bigint> => ({
    arrowDataType: new Uint64(),
    validate(value) {
        assert(typeof value === 'bigint')
        return value
    },
})

export let DecimalType = (scale: number, precision: number): ParquetType<number> => ({
    arrowDataType: new Decimal(scale, precision),
    validate(value) {
        assert(Number.isInteger(value) && typeof value === 'number')
        return value
    },
})

export let TimestampType = (
    unit: 'seconds' | 'millis' | 'micros' | 'nanos',
    useTimezone: boolean = true
): ParquetType<number> => ({
    arrowDataType: new Timestamp(
        unit === 'seconds'
            ? TimeUnit.SECOND
            : unit === 'millis'
            ? TimeUnit.MILLISECOND
            : unit === 'micros'
            ? TimeUnit.MICROSECOND
            : TimeUnit.NANOSECOND,
        useTimezone ? Intl.DateTimeFormat().resolvedOptions().timeZone : null
    ),
    validate(value) {
        // assert(value instanceof Date)
        return value as number
    },
})

export let TimeType = (unit: 'millis' | 'micros' | 'nanos'): ParquetType<number> => ({
    arrowDataType: new Time(
        unit === 'millis' ? TimeUnit.MILLISECOND : unit === 'micros' ? TimeUnit.MICROSECOND : TimeUnit.NANOSECOND,
        unit === 'millis' ? 32 : 64
    ),
    validate(value) {
        // assert(Number.isInteger(value) && typeof value === 'number')
        return value as number
    },
})

export let DateType = (): ParquetType<Date> => ({
    arrowDataType: new Date_(DateUnit.DAY),
    validate(value) {
        assert(value instanceof Date)
        return value
    },
})

export let IntervalType = (): ParquetType<Uint32Array> => ({
    arrowDataType: new Interval(IntervalUnit.DAY_TIME),
    validate(value) {
        // assert(value instanceof Date)
        return value as any
    },
})

// type a = Interval['TValue']
