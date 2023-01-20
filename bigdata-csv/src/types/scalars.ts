import assert from 'assert'
import {CsvType} from './type'

export function StringType(): CsvType<string> {
    return {
        serialize(value) {
            return this.validate(value)
        },
        validate(value) {
            assert(typeof value === 'string')
            return value
        },
        isNumeric: false,
    }
}

export function IntegerType(): CsvType<number | bigint> {
    return {
        serialize(value: number) {
            return this.validate(value).toString()
        },
        validate(value) {
            assert((typeof value === 'number' && Number.isInteger(value)) || typeof value === 'bigint')
            return value
        },
        isNumeric: true,
    }
}

export function DecimalType(): CsvType<number> {
    return {
        serialize(value: number) {
            return this.validate(value).toString()
        },
        validate(value) {
            assert(typeof value === 'number')
            return value
        },
        isNumeric: true,
    }
}
export function BooleanType(): CsvType<boolean> {
    return {
        serialize(value: boolean) {
            assert(typeof value === 'boolean', 'Invalid boolean')
            return this.validate(value).toString()
        },
        validate(value) {
            assert(typeof value === 'boolean')
            return value
        },
        isNumeric: false,
    }
}

export function DateTimeType(): CsvType<Date> {
    return {
        serialize(value: Date) {
            return this.validate(value).toISOString()
        },
        validate(value) {
            assert(value instanceof Date)
            return value
        },
        isNumeric: false,
    }
}
