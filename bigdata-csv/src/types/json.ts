import {CsvType} from './type'
import {toJSON} from '@subsquid/util-internal-json'

export let JSONType = <T>(): CsvType<T> => ({
    serialize(value) {
        return JSON.stringify(toJSON(value))
    },
    validate(value: unknown) {
        return value as T
    },
})
