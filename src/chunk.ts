import {assertNotNull} from '@subsquid/util-internal'
import {toSnakeCase} from '@subsquid/util-naming'
import assert from 'assert'
import {Table, TableHeader, TableRecord} from './table'
import {Dialect} from './util/dialect'

export class Chunk {
    private tableBuilders: Map<string, TableBuilder<any>>

    constructor(public from: number, public to: number, tables: Table<any>[]) {
        this.tableBuilders = new Map(tables.map((t) => [t.name, new TableBuilder(t)]))
    }

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tableBuilders.get(name), `Table "${name}" does not exist`)
    }

    get size() {
        let total = 0
        for (let table of this.tableBuilders.values()) {
            total += table.size
        }
        return total
    }
}

export interface TableOutputOptions {
    dialect: Dialect
    header: boolean
}

export class TableBuilder<T extends TableHeader> {
    private records: string[][] = []
    private _size = 0

    constructor(private table: Table<T>) {}

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
                header[i] = this.hasSpecialChar(normalizedName, options.dialect) ? `'${normalizedName}'` : normalizedName
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

    append(records: TableRecord<T> | TableRecord<T>[]): void {
        records = Array.isArray(records) ? records : [records]

        for (let record of records) {
            let values: string[] = []
            for (let field of this.table.fields) {
                let value = record[field.name]
                assert(value != null || field.data.nullable, `Null value in non-nullable field "${field.name}"`)
                let serializedValue = value == null ? `` : field.data.type.serialize(value)
                values.push(serializedValue)
                this._size += Buffer.byteLength(serializedValue)
            }
            this.records.push(values)
        }
    }

    private hasSpecialChar(str: string, dialect: Dialect) {
        return str.includes(dialect.delimiter) || str.includes('\n') || str.includes(dialect.quoteChar)
    }
}
