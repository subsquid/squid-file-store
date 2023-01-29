import {DataType} from 'apache-arrow'

export interface Type<T> {
    arrowDataType: DataType
    prepare(value: T): any
}
