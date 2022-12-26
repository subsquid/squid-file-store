import {assert} from 'console'
import * as scalars from './scalars'
import {ConvertFieldsToTypes, Field, getFieldData} from './utils'

export type TableHeader = Record<string, Field>

export class Table<T extends TableHeader> {
    constructor(readonly name: string, readonly header: T) {}

    serializeFieldTypes() {
        let fields = this.getFields()
        let res = new Array(fields.length)
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i]
            let type = field.data.type.dbType
            if (!field.data.nullable) type += ` NOT NULL`
            res[i] = `"${field.name}" ${type}`
        }
        return res
    }

    serializeRecord(record: TableRecord<T>) {
        let fields = this.getFields()
        let res = new Array(fields.length)
        for (let i = 0; i < fields.length; i++) {
            let field = fields[i]
            let value = record[field.name]
            let serializedValue = value == null ? null : field.data.type.serialize(value)
            res[i] = serializedValue
        }
        return res
    }

    private getFields() {
        return Object.entries(this.header).map(([name, fieldData]) => ({
            name,
            data: getFieldData(fieldData),
        }))
    }
}

export type TableRecord<T extends TableHeader | Table<any>> = T extends Table<infer R>
    ? ConvertFieldsToTypes<R>
    : T extends TableHeader
    ? ConvertFieldsToTypes<T>
    : never

export let types = {
    string: scalars.StringType,
    tinyInt: scalars.TinyIntType,
    smallInt: scalars.SmallIntType,
    int: scalars.IntType,
    bigInt: scalars.BigIntType,
    hugeInt: scalars.HugeIntType,
    uTinyInt: scalars.UTinyIntType,
    uSmallInt: scalars.USmallIntType,
    uInt: scalars.UIntType,
    uBigInt: scalars.UBigIntType,
    real: scalars.RealType,
    double: scalars.DoubleType,
    bytes: scalars.BlobType,
    date: scalars.DateType,
    timestamp: scalars.TimestampType,
    timestampz: scalars.TimestampZType,
    boolean: scalars.BooleanType,
}

export * from './utils'
export * from './scalars'
export * from './struct'
export * from './list'
