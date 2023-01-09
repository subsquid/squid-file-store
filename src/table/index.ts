import * as scalars from './scalars'
import {ConvertFieldsToTypes, Field, FieldData, getFieldData} from './utils'

export type TableHeader = Record<string, Field>

export class Table<T extends TableHeader> {
    fields: {name: string; data: FieldData}[] = []

    constructor(readonly name: string, header: T) {
        for (let field in header) {
            this.fields.push({
                name: field,
                data: getFieldData(header[field]),
            })
        }
    }
}

export type TableRecord<T extends TableHeader | Table<any>> = T extends Table<infer R>
    ? ConvertFieldsToTypes<R>
    : T extends TableHeader
    ? ConvertFieldsToTypes<T>
    : never

export let types = {
    string: scalars.StringType(),
    number: scalars.NumberType(),
    bigint: scalars.BigIntType(),
    timestamp: scalars.TimestampType(),
    boolean: scalars.BooleanType(),
}

export * from './utils'
export * from './scalars'
export * from './list'
