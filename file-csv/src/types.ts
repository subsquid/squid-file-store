import {toJSON} from '@subsquid/util-internal-json'
import assert from 'assert'
import strftime from 'strftime'
import {Type} from './table'

/**
 * @returns the data type for string columns
 */
export function String(): Type<string> {
    return {
        serialize(value) {
            assert(typeof value === 'string')
            return value
        },
        isNumeric: false,
    }
}

/**
 * @returns the data type for numeric columns
 */
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

/**
 * @returns the data type for boolean columns
 */
export function Boolean(): Type<boolean> {
    return {
        serialize(value: boolean) {
            assert(typeof value === 'boolean', 'Invalid boolean')
            return value.toString()
        },
        isNumeric: false,
    }
}

/**
 * @param format - a strftime-compatible data format string. ISO format is used if undefined.
 *
 * @returns the data type for time and date columns
 */
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

/**
 * Supply the type of column JSON as the generic parameter.
 *
 * @returns the data type for JSON-valued columns
 *
 * @example
 * ```
 * JSON<{from: string, to: string, value: bigint}>()
 * ```
 */
export function JSON<T extends Document = any>(): Type<T> {
    return {
        serialize(value: T) {
            return global.JSON.stringify(toJSON(value))
        },
        isNumeric: false,
    }
}
