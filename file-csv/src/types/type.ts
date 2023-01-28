import {Type} from '@subsquid/file-table'

export interface CsvType<T> extends Type<T> {
    serialize(value: T): string
    isNumeric: boolean
}
