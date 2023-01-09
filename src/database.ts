import assert from 'assert'
import {Dialect, dialects} from './util/dialect'
import {createFS, FS, S3Options} from './util/fs'
import {Table, TableHeader, TableRecord} from './table'
import {Chunk} from './chunk'

interface CsvOutputOptions {
    /**
     * Output files extension.
     * @Default 'csv'
     */
    extension?: string
    /**
     * @Default excel
     */
    dialect?: Dialect
    /**
     * @Default true
     */
    header?: boolean
}

export interface CsvDatabaseOptions {
    /**
     * Local or s3 destination. For s3 use 's3://bucket/path'
     * @Default ./data
     */
    dest?: string
    /**
     * Minimal folder size (MB).
     * @Default 20
     */
    chunkSize?: number

    updateInterval?: number
    /**
     * Options for different file systems. Only s3 options supported now.
     */
    s3Options?: S3Options

    outputOptions?: CsvOutputOptions
}

interface DatabaseStatus {
    height: number
    chunks: string[]
}

export class CsvDatabase {
    protected dest: string
    protected chunkSize: number
    protected updateInterval: number
    protected s3Options?: S3Options
    protected outputOptions: Required<CsvOutputOptions>

    protected fs: FS
    protected chunk?: Chunk
    protected status?: DatabaseStatus

    constructor(private tables: Table<any>[], options?: CsvDatabaseOptions) {
        this.dest = options?.dest || './data'
        this.chunkSize = options?.chunkSize && options.chunkSize > 0 ? options.chunkSize : 20
        this.updateInterval = options?.updateInterval && options.updateInterval > 0 ? options.updateInterval : Infinity
        this.s3Options = options?.s3Options
        this.outputOptions = {extension: 'csv', header: true, dialect: dialects.excel, ...options?.outputOptions}

        this.fs = createFS(this.dest, this.s3Options)
    }

    async connect(): Promise<number> {
        if (await this.fs.exist(`status.json`)) {
            let status: DatabaseStatus = await this.fs.readFile(`status.json`).then(JSON.parse)
            this.status = status
        } else {
            this.status = {
                height: -1,
                chunks: [],
            }
        }
        return this.status.height
    }

    async close(): Promise<void> {
        this.chunk = undefined
        this.status = undefined
    }

    async transact(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let open = true

        if (this.chunk == null) {
            this.chunk = new Chunk(from, to, this.tables, this.outputOptions)
        } else {
            this.chunk.to = to
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

    async advance(height: number, isHead?: boolean): Promise<void> {
        assert(this.status != null, `Not connected to database`)

        if (this.chunk == null) return

        if (this.chunk.to < height) {
            this.chunk.to = height
        }

        if (
            this.chunk.size >= this.chunkSize * 1024 * 1024 ||
            (isHead && height - this.chunk.from >= this.updateInterval)
        ) {
            let folderName =
                this.chunk.from.toString().padStart(10, '0') + '-' + this.chunk.to.toString().padStart(10, '0')
            await this.outputChunk(folderName, this.chunk)
            this.status.height = height
            this.status.chunks.push(folderName)
            await this.fs.writeFile(`status.json`, JSON.stringify(this.status, null, 4))
            this.chunk = undefined
        }
    }

    private async outputChunk(path: string, chunk: Chunk) {
        await this.fs.transact(path, async (txFs) => {
            for (let table of this.tables) {
                let tablebuilder = chunk.getTableBuilder(table.name)
                await txFs.writeFile(`${table.name}.${this.outputOptions.extension}`, tablebuilder.data)
            }
        })
    }
}

export class Store {
    constructor(private chunk: () => Chunk) {}

    write<T extends TableHeader>(table: Table<T>, records: TableRecord<T> | TableRecord<T>[]): void {
        let builder = this.chunk().getTableBuilder(table.name)
        builder.append(records)
    }
}
