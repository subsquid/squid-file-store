import assert from 'assert'
import {Dialect, dialects} from './dialect'
import {createFS, FS, FSOptions} from './fs'
import {Chunk, Table, TableBuilder, TableHeader, TableRecord} from './table'
import {types} from './types'

const PENDING_FOLDER = 'last'
const STATUS_TABLE = 'status'

export interface CsvDatabaseOptions {
    /**
     * Local or s3 destination.
     * For s3 use "s3://*bucketName/path"
     * @Default ./data
     */
    dest?: string
    /**
     * @Default utf-8
     */
    encoding?: BufferEncoding
    /**
     * Output files extension.
     * @Default csv
     */
    extension?: string
    /**
     * @Default excel
     */
    dialect?: Dialect
    /**
     * Minimal folder size (MB).
     * @Default 20
     */
    chunkSize?: number
    /**
     * Pending data output interval (blocks).
     */
    updateInterval?: number
    /**
     * Options for different file systems. Only s3 options supported now.
     */
    fsOptions?: FSOptions
}

export class CsvDatabase {
    private encoding: BufferEncoding
    private extension: string
    private chunkSize: number
    private updateInterval: number
    private dialect: Dialect
    private lastOutputed = -1
    private chunk: Chunk | undefined
    private fs: FS

    constructor(private tables: Table<any>[], options?: CsvDatabaseOptions) {
        this.extension = options?.extension || 'csv'
        this.encoding = options?.encoding || 'utf-8'
        this.dialect = options?.dialect || dialects.excel
        this.chunkSize = options?.chunkSize || 20
        this.fs = createFS(options?.dest || './data', options?.fsOptions)
        this.updateInterval = options?.updateInterval && options.updateInterval > 0 ? options.updateInterval : Infinity
    }

    async connect(): Promise<number> {
        await this.fs.remove(PENDING_FOLDER)
        if (await this.fs.exist(`${STATUS_TABLE}.${this.extension}`)) {
            let rows = await this.fs
                .readFile(`${STATUS_TABLE}.${this.extension}`, this.encoding)
                .then((data) => data.split(this.dialect.lineTerminator))
            this.lastOutputed = Number(rows[2])
        } else {
            await this.updateHeight(-1)
            this.lastOutputed = -1
        }
        return this.lastOutputed
    }

    async close(): Promise<void> {
        this.chunk = undefined
        this.lastOutputed = -1
    }

    async transact(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let retries = 3
        while (true) {
            try {
                return await this.runTransaction(from, to, cb)
            } catch (e: any) {
                if (retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    private async runTransaction(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let open = true

        if (!this.chunk) {
            this.chunk = this.createChunk(from, to)
        } else {
            this.chunk.expandRange(to)
        }

        let store = new Store(() => {
            assert(open, `Transaction was already closed`)
            return this.chunk as Chunk
        })

        try {
            await cb(store)
        } catch (e: any) {
            open = false
            throw e
        }

        open = false
    }

    async advance(height: number): Promise<void> {
        if (!this.chunk) return

        this.chunk.expandRange(height)
        if (this.chunk.getSize(this.encoding) >= this.chunkSize * 1024 * 1024) {
            await this.outputChunk(this.chunk.name, this.chunk)
            await this.updateHeight(height)
            await this.fs.remove(PENDING_FOLDER)
            this.chunk = undefined
            this.lastOutputed = height
        } else if (height - this.lastOutputed >= this.updateInterval) {
            await this.outputChunk(PENDING_FOLDER, this.chunk)
            this.lastOutputed = height
        }
    }

    private createChunk(from: number, to: number) {
        return new Chunk(from, to, new Map(this.tables.map((t) => [t.name, new TableBuilder(t.header, this.dialect)])))
    }

    private async outputChunk(path: string, chunk: Chunk) {
        await this.fs.transact(path, async (txFs) => {
            for (let table of this.tables) {
                let tablebuilder = chunk.getTableBuilder(table.name)
                await txFs.writeFile(`${table.name}.${this.extension}`, tablebuilder.getTable(), this.encoding)
            }
        })
    }

    private async updateHeight(height: number) {
        let statusTable = new TableBuilder({height: types.int}, this.dialect, [{height}])
        await this.fs.writeFile(`${STATUS_TABLE}.${this.extension}`, statusTable.getTable(), this.encoding)
    }
}

export class Store {
    constructor(private _tables: () => Chunk) {}

    private get tables() {
        return this._tables()
    }

    write<T extends TableHeader>(table: Table<T>, records: TableRecord<T> | TableRecord<T>[]): void {
        let builder = this.tables.getTableBuilder(table.name)
        builder.append(records)
    }
}
