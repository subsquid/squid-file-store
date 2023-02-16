import assert from 'assert'
import {Type} from '../table'

export function String(): Type<string> {
    return {
        serialize(value) {
            return value
        },
        validate(value) {
            assert(typeof value === 'string')
            return value
        },
        isNumeric: false,
    }
}

export function Integer(): Type<number | bigint> {
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

export function Decimal(): Type<number> {
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
export function Boolean(): Type<boolean> {
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

export function Timestamp(): Type<Date> {
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

/**
 * @deprecated use Timestamp
 */
export const DateTime = Timestamp
