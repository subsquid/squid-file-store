import {assertNotNull} from '@subsquid/util-internal'
import assert from 'assert'
import {Dialect, dialects} from './dialect'
import {createFS, FS, FSOptions} from './fs'
import {Chunk, Table, TableBuilder, TableHeader, TableRecord} from './table'
import {types} from './types'

const PENDING_FOLDER = 'last'
const STATUS_TABLE = 'status.csv'

export interface CsvDatabaseOptions {
    dest?: string
    encoding?: BufferEncoding
    extension?: string
    dialect?: Dialect
    chunkSize?: number
    updateInterval?: number
    fsOptions?: FSOptions
}

export class CsvDatabase {
    private encoding: BufferEncoding
    private extension: string
    private chunkSize: number
    private updateInterval: number
    private dialect: Dialect
    private lastCommitted = -1
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
        if (await this.fs.exist(STATUS_TABLE)) {
            let rows = await this.fs
                .readFile(STATUS_TABLE, this.encoding)
                .then((data) => data.split(dialects.excel.lineTerminator))
            this.lastCommitted = Number(rows[2])
        } else {
            await this.updateHeight(-1)
            this.lastCommitted = -1
        }
        return this.lastCommitted
    }

    async close(): Promise<void> {
        this.chunk = undefined
        this.lastCommitted = -1
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
            this.chunk.changeRange({to: to})
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

        this.chunk.changeRange({to: height})

        if (this.chunk.getSize(this.encoding) >= this.chunkSize * 1024 * 1024) {
            await this.fs.transact(this.chunk.name, async (txFs) => {
                for (let table of this.tables) {
                    let tablebuilder = assertNotNull(this.chunk).getTableBuilder(table.name)
                    await txFs.writeFile(`${table.name}.${this.extension}`, tablebuilder.getTable(), this.encoding)
                }
            })
            await this.fs.remove(PENDING_FOLDER)
            await this.updateHeight(height)
            this.chunk = undefined
            this.lastCommitted = height
        } else if (height - this.lastCommitted >= this.updateInterval) {
            await this.fs.transact(PENDING_FOLDER, async (txFs) => {
                for (let table of this.tables) {
                    let tablebuilder = assertNotNull(this.chunk).getTableBuilder(table.name)
                    await txFs.writeFile(`${table.name}.${this.extension}`, tablebuilder.getTable(), this.encoding)
                }
            })
            this.lastCommitted = height
        }
    }

    private createChunk(from: number, to: number) {
        return new Chunk(from, to, new Map(this.tables.map((t) => [t.name, new TableBuilder(t.header, this.dialect)])))
    }

    private async updateHeight(height: number) {
        let statusTable = new TableBuilder({height: types.int}, dialects.excel, [{height}])
        await this.fs.writeFile(STATUS_TABLE, statusTable.getTable(), this.encoding)
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
