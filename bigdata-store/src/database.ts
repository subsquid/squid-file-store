import assert from 'assert'
import {createFS, FS, fsTransact, S3Options} from './util/fs'
import {Chunk} from './chunk'
import {Table, TableSchema, TableRecord} from '@subsquid/bigdata-table'

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

    hooks?: DatabaseHooks
}

export class CsvDatabase {
    protected tables: Table<any>[]

    protected dest: string
    protected chunkSize: number
    protected updateInterval: number
    protected s3Options?: S3Options

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
        this.hooks = options.hooks || defaultHooks

        this.fs = createFS(this.dest, this.s3Options)
    }

    async connect(): Promise<number> {
        this.lastBlock = await this.hooks.onConnect(this.fs)

        let names = await this.fs.readdir('./')
        for (let name of names) {
            if (!/^(\d+)-(\d+)$/.test(name)) continue

            let chunkStart = Number(name.split('-')[0])
            if (chunkStart > this.lastBlock) {
                await this.fs.rm(name)
            }
        }

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
            this.chunk = undefined

            await this.hooks.onFlush(this.fs, height, isHead ?? false)
        }
    }

    private async outputChunk(path: string, chunk: Chunk) {
        await fsTransact(this.fs, path, async (txFs) => {
            for (let table of this.tables) {
                let tablebuilder = chunk.getTableBuilder(table.name)
                await txFs.writeFile(
                    `${table.name}.${table.getFileExtension()}`,
                    tablebuilder.toTable()
                )
            }
        })
    }
}

export class Store {
    constructor(private chunk: () => Chunk) {}

    write<T extends TableSchema<any>>(table: Table<T>, records: TableRecord<T> | TableRecord<T>[]): void {
        let builder = this.chunk().getTableBuilder(table.name)
        builder.append(records)
    }
}

const defaultHooks: DatabaseHooks = {
    async onConnect(fs) {
        if (await fs.exists(`status.txt`)) {
            let height = await fs.readFile(`status.json`).then(Number)
            assert(Number.isSafeInteger(height))
            return height
        } else {
            return -1
        }
    },
    async onFlush(fs, height) {
        await fs.writeFile(`status.txt`, height.toString())
    },
}
