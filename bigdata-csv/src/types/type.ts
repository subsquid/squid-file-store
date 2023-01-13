import {Type} from '@subsquid/bigdata-table'

export interface CsvType<T> extends Type<T> {
    serialize(value: T): string
}
