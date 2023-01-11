import assert from 'assert'
import {Dialect, dialects} from './util/dialect'
import {createFS, FS, fsTransact, S3Options} from './util/fs'
import {Table, TableHeader, TableRecord} from './table'
import {Chunk} from './chunk'
import {getSystemErrorMap} from 'util'

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

interface DatabaseHooks {
    onConnect(fs: FS): Promise<number>
    onFlush(fs: FS, height: number, isHead: boolean): Promise<void>
}

export interface CsvDatabaseOptions {
    tables: Table<any>[]
    /**
     * Local or s3 destination. For s3 use 's3://bucket/path'
     * @Default ./data
     */
    dest?: string
    /**
     * Approximate folder size (MB).
     * @Default 20
     */
    chunkSizeMb?: number
    /**
     * How often output result after reaching chain head (blocks).
     * @Default Infinity
     */
    syncIntervalBlocks?: number
    /**
     * S3 connection options.
     */
    s3Options?: S3Options

    outputOptions?: CsvOutputOptions

    hooks?: DatabaseHooks
}

export class CsvDatabase {
    protected tables: Table<any>[]

    protected dest: string
    protected chunkSize: number
    protected updateInterval: number
    protected s3Options?: S3Options
    protected outputOptions: Required<CsvOutputOptions>

    protected fs: FS
    protected chunk?: Chunk
    protected lastBlock?: number

    protected hooks: DatabaseHooks

    constructor(options: CsvDatabaseOptions) {
        this.tables = options.tables
        this.dest = options?.dest || './data'
        this.chunkSize = options?.chunkSizeMb && options.chunkSizeMb > 0 ? options.chunkSizeMb : 20
        this.updateInterval =
            options?.syncIntervalBlocks && options.syncIntervalBlocks > 0 ? options.syncIntervalBlocks : Infinity
        this.s3Options = options?.s3Options
        this.outputOptions = {extension: 'csv', header: true, dialect: dialects.excel, ...options?.outputOptions}
        this.hooks = options.hooks || defaultHooks

        this.fs = createFS(this.dest, this.s3Options)
    }

    async connect(): Promise<number> {
        this.lastBlock = await this.hooks.onConnect(this.fs)
        return this.lastBlock
    }

    async close(): Promise<void> {
        this.chunk = undefined
        this.lastBlock = undefined
    }

    async transact(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let open = true

        if (this.chunk == null) {
            this.chunk = new Chunk(from, to, this.tables)
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
        assert(this.lastBlock != null, `Not connected to database`)

        if (this.chunk == null) return

        if (this.chunk.to < height) {
            this.chunk.to = height
        }

        if (
            this.chunk.size >= this.chunkSize * 1024 * 1024 ||
            (isHead && height - this.chunk.from >= this.updateInterval && this.chunk.size > 0)
        ) {
            let folderName =
                this.chunk.from.toString().padStart(10, '0') + '-' + this.chunk.to.toString().padStart(10, '0')
            await this.outputChunk(folderName, this.chunk)
            this.lastBlock = height
            await this.hooks.onFlush(this.fs, height, isHead ?? false)
            this.chunk = undefined
        }
    }

    private async outputChunk(path: string, chunk: Chunk) {
        await fsTransact(this.fs, path, async (txFs) => {
            for (let table of this.tables) {
                let tablebuilder = chunk.getTableBuilder(table.name)
                await txFs.writeFile(
                    `${table.name}.${this.outputOptions.extension}`,
                    tablebuilder.toTable(this.outputOptions)
                )
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

const defaultHooks: DatabaseHooks = {
    async onConnect(fs) {
        if (await fs.exists(`status.txt`)) {
            let height = await fs.readFile(`status.json`).then(Number)
            assert(Number.isNaN(height))
            return height
        } else {
            return -1
        }
    },
    async onFlush(fs, height, isHead) {
        await fs.writeFile(`status.txt`, height.toString())
    },
}
