import {Table as ITable, TableWriter as ITableWriter} from '@subsquid/file-store'
import {toJSON} from '@subsquid/util-internal-json'

type TableOptions = {
    /**
     * Should the JSONL format be used instead of a plain JSON array of records?
     */
    lines?: boolean
}

/**
 * Table interface implementation for writing JSON and JSONL files.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/json-table/
 */
export class Table<S extends Record<string, any>> implements ITable<S> {
    private options: Required<TableOptions>

    /**
     * Table interface implementation for writing JSON and JSONL files.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/json-table/
     *
     * @param name - name of the output file in every dataset partition folder
     * @param options - table options
     *
     * @example
     * ```
     * import {Table} from '@subsquid/file-store-json'
     *
     * let transfersTable = new Table<
     *     from: string,
     *     to: string,
     *     value: bigint
     * >('transfers.jsonl', {lines: true})
     * ```
     */
    constructor(readonly name: string, options?: TableOptions) {
        this.options = {lines: false, ...options}
    }

    createWriter(): TableWriter<S> {
        return new TableWriter(this.options)
    }
}

class TableWriter<T extends Record<string, any>> implements ITableWriter<T> {
    private records: string[] = []
    private _size = 0

    constructor(private options: Required<TableOptions>) {}

    get size() {
        return this._size
    }

    flush(): Uint8Array {
        let res: Buffer
        if (this.options.lines) {
            res = Buffer.from(this.records.join('\n'), 'utf-8')
        } else {
            res = Buffer.from(`[${this.records.join(',')}]`, 'utf-8')
        }
        this.reset()

        return res
    }

    reset() {
        this.records = []
        this._size = 0
    }

    write(record: T): this {
        return this.writeMany([record])
    }

    writeMany(records: T[]): this {
        for (let record of records) {
            let str = JSON.stringify(toJSON(record))
            this.records.push(str)
            this._size += str.length
        }

        return this
    }
}
