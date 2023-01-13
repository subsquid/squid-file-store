import {toSnakeCase} from '@subsquid/util-naming'
import assert from 'assert'
import {
    TableHeader,
    Table,
    TableRecord,
    TableBuilder,
    FieldData,
    Type,
    Field,
    ConvertFieldsToTypes,
} from '@subsquid/bigdata-table'
import {Dialect} from './dialect'
import {CsvType} from './types'

export interface TableOutputOptions {
    dialect: Dialect
    header: boolean
}

export interface CsvTableHeader extends TableHeader {
    [field: string]: Field<CsvType<any>>
}

export class CsvTable<T extends CsvTableHeader> extends Table<T> {
    createTableBuilder(): CsvTableBuilder<T> {
        return new CsvTableBuilder(this)
    }
}

export class CsvTableBuilder<T extends CsvTableHeader> implements TableBuilder<T> {
    private records: string[][] = []
    private _size = 0

    constructor(private table: CsvTable<T>) {}

    get size() {
        return this._size
    }

    toTable(options: TableOutputOptions) {
        let res = new Array<string>(options.header ? this.records.length + 1 : this.records.length)

        if (options.header) {
            let header = new Array<string>(this.table.fields.length)
            for (let i = 0; i < this.table.fields.length; i++) {
                let field = this.table.fields[i]
                let normalizedName = toSnakeCase(field.name)
                header[i] = this.hasSpecialChar(normalizedName, options.dialect)
                    ? `'${normalizedName}'`
                    : normalizedName
            }
            res[0] = header.join(options.dialect.delimiter)
        }

        for (let i = res.length; i < this.records.length; i++) {
            let serializedRecord = new Array<string>(this.table.fields.length)
            for (let j = 0; j < this.table.fields.length; j++) {
                let value = this.records[i][j]
                serializedRecord[j] = this.hasSpecialChar(value, options.dialect) ? `'${value}'` : value
            }
            res[i] = serializedRecord.join(options.dialect.delimiter)
        }

        return this.records.join('\n')
    }

    append(records: TableRecord<T> | TableRecord<T>[]): TableBuilder<T> {
        records = Array.isArray(records) ? records : [records]

        for (let record of records) {
            let values: string[] = []
            for (let field of this.table.fields) {
                let value = record[field.name]
                if (value == null) {
                    assert(value != null || field.data.nullable, `Null value in non-nullable field "${field.name}"`)
                } else {
                    field.data.type.validate(value)
                }
                let serializedValue = value == null ? `` : field.data.type.serialize(value)
                values.push(serializedValue)
                this._size += Buffer.byteLength(serializedValue)
            }
            this.records.push(values)
        }

        return this
    }

    private hasSpecialChar(str: string, dialect: Dialect) {
        return str.includes(dialect.delimiter) || str.includes('\n') || str.includes(dialect.quoteChar)
    }
}

// let type: CsvType<string> = {
//     validate: (v) => v,
//     serialize: () => 'a',
// }

// let a = new CsvTable('aaa', {
//     a: type,
// })

// type B = typeof a extends Table<infer R> ? R : never

// type A = TableRecord<typeof a>
