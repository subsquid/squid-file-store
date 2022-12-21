import {assertNotNull} from '@subsquid/util-internal'
import {Dialect} from './dialect'
import {Type} from './types'

export type TableHeader = {
    [k: string]: Type<any>
}

export class Table<T extends TableHeader> {
    constructor(readonly name: string, readonly header: T) {}
}

type NullableFields<T extends TableHeader> = {
    [k in keyof T]: T[k] extends Type<infer R> ? (null extends R ? k : never) : never
}[keyof T]

export type TableRecord<T extends TableHeader | Table<any>> = T extends Table<infer R>
    ? ConvertFieldsToTypes<R>
    : T extends TableHeader
    ? ConvertFieldsToTypes<T>
    : never

type ConvertFieldsToTypes<T extends TableHeader> = {
    [k in keyof Omit<T, NullableFields<T>>]: T[k] extends Type<infer R> ? R : never
} & {
    [k in keyof Pick<T, NullableFields<T>>]?: T[k] extends Type<infer R> ? R : never
}

export class TableBuilder<T extends TableHeader> {
    private records: string[] = []

    constructor(private header: TableHeader, private dialect: Dialect, records: TableRecord<T>[] = []) {
        if (this.dialect.header) {
            let serializedHeader = Object.keys(this.header).join(this.dialect.delimiter) + this.dialect.lineTerminator
            this.records.push(serializedHeader)
        }
        this.append(records)
    }

    getSize(encoding: BufferEncoding) {
        let size = 0
        for (let record of this.records) {
            size += Buffer.byteLength(record, encoding)
        }
        return size
    }

    toTable() {
        return this.records.join('')
    }

    append(records: TableRecord<T> | TableRecord<T>[]): void {
        records = Array.isArray(records) ? records : [records]
        for (let record of records) {
            let serializedRecord = this.serializeRecord(record) + this.dialect.lineTerminator
            this.records.push(serializedRecord)
        }
    }

    private serializeRecord(record: TableRecord<T>) {
        let fields = Object.entries(this.header)
        let serializedFields = new Array<string>(fields.length)
        for (let i = 0; i < fields.length; i++) {
            let [fieldName, fieldData] = fields[i]
            serializedFields[i] = fieldData.serialize(record[fieldName], this.dialect)
        }
        return serializedFields.join(this.dialect.delimiter)
    }
}

export class Chunk {
    constructor(private from: number, private to: number, private tables: Map<string, TableBuilder<any>>) {}

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tables.get(name), `Table ${name} does not exist`)
    }

    expandRange(to: number) {
        this.to = to
    }

    getSize(encoding: BufferEncoding) {
        let total = 0
        for (let table of this.tables.values()) {
            total += table.getSize(encoding)
        }
        return total
    }

    get name() {
        return `${this.from.toString().padStart(10, '0')}-${this.to.toString().padStart(10, '0')}`
    }
}
