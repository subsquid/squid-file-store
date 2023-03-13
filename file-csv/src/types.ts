import {toJSON} from '@subsquid/util-internal-json'
import assert from 'assert'
import strftime from 'strftime'
import {Type} from './table'

export function String(): Type<string> {
    return {
        serialize(value) {
            assert(typeof value === 'string')
            return value
        },
        isNumeric: false,
    }
}

export function Numeric(): Type<number | bigint> {
    return {
        serialize(value: number) {
            assert(typeof value === 'number' || typeof value === 'bigint')
            return value.toString()
        },
        isNumeric: true,
    }
}

/**
 * @deprecated use Numeric
 */
export const Integer = Numeric

/**
 * @deprecated use Numeric
 */
export const Decimal = Numeric

export function Boolean(): Type<boolean> {
    return {
        serialize(value: boolean) {
            assert(typeof value === 'boolean', 'Invalid boolean')
            return value.toString()
        },
        isNumeric: false,
    }
}

export function DateTime(format?: string): Type<Date> {
    return {
        serialize(value: Date) {
            assert(value instanceof Date)
            if (format == null) {
                return value.toISOString()
            } else {
                return strftime(format, value)
            }
        },
        isNumeric: false,
    }
}

/**
 * @deprecated use DateTime
 */
export const Timestamp = DateTime

type Document = {
    [k: string]: any
}

export function JSON<T extends Document = any>(): Type<T> {
    return {
        serialize(value: T) {
            return global.JSON.stringify(toJSON(value))
        },
        isNumeric: false,
    }
}
