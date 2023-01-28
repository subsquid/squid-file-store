import {Type} from '@subsquid/file-table'
import {DataType} from 'apache-arrow'

export interface ParquetType<T> extends Type<T> {
    arrowDataType: DataType
    prepare(value: T): any
}