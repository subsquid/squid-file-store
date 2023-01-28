import assert from 'assert'
import {Table, TableRecord, TableWriter} from './table'
import {Chunk} from './chunk'
import {FS, S3Options, createFS, fsTransact} from './util/fs'

interface DatabaseHooks {
    onConnect(fs: FS): Promise<number>
    onFlush(fs: FS, height: number, isHead: boolean): Promise<void>
}

export interface DatabaseOptions<T extends Record<string, Table<any>>> {
    tables: T
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

export class Database<T extends Record<string, Table<any>>> {
    protected tables: T

    protected dest: string
    protected chunkSize: number
    protected updateInterval: number
    protected s3Options?: S3Options

    protected fs: FS
    protected chunk?: Chunk
    protected lastBlock?: number

    protected hooks: DatabaseHooks

    constructor(options: DatabaseOptions<T>) {
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

    async transact(from: number, to: number, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let open = true

        if (this.chunk == null) {
            this.chunk = new Chunk(from, to, this.tables)
        } else {
            this.chunk.to = to
        }

        let store = new Store(() => {
            assert(open, `Transaction was already closed`)
            return this.chunk!.writers
        })

        try {
            await cb(store as any)
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
            await fsTransact(this.fs, folderName, async (txFs) => {
                for (let name in this.tables) {
                    await txFs.writeFile(`${this.tables[name].name}`, this.chunk!.writers[name].flush())
                }
            })
            this.lastBlock = height
            this.chunk = undefined

            await this.hooks.onFlush(this.fs, height, isHead ?? false)
        }
    }
}

export class Store<T extends Record<string, Table<any>>> {
    constructor(
        private writers: () => {
            readonly [k in keyof T]: Pick<TableWriter<T[k] extends Table<infer R> ? R : never>, 'write' | 'writeMany'>
        }
    ) {}

    get tables() {
        return this.writers()
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
