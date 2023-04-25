import assert from 'assert'
import {Table, TableWriter} from './table'
import {Dest, LocalDest} from './dest'

type Range = {from: number; to: number}

interface DatabaseHooks<D extends Dest = Dest> {
    onConnect(dest: D): Promise<number>
    onFlush(dest: D, range: Range, isHead: boolean): Promise<void>
}

type Tables = Record<string, Table<any>>

export interface DatabaseOptions<T extends Tables, D extends Dest> {
    /**
     * A mapping from table handles to Table instances. For each such pair
     * a TableWriter will be added to BatchContext.store to enable storage of
     * table rows.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/overview/#database-options
     *
     * @example
     * This adds a `ctx.store.transfersTable` table writer to the batch
     * context store.
     * ```
     * import {
     *     Table,
     *     Column,
     *     Types
     * } from '@subsquid/file-store-csv'
     *
     * tables: {
     *     transfersTable: new Table('transfers.csv', {
     *         from: Column(Types.String()),
     *         to: Column(Types.String()),
     *         value: Column(Types.Integer())
     *     }
     * },
     * ```
     */
    tables: T

    /**
     * A Dest object defining the filesystem connection.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/overview/#database-options
     *
     * @example
     * Write the data to a local './data' folder
     * ```
     * import {LocalDest} from '@subsquid/file-store'
     *
     * dest: LocalDest('./data')
     * ```
     */
    dest: D

    /**
     * Amount of in-memory data that will trigger a filesystem
     * write. Roughly defines the dataset partition size.
     *
     * Unit: Megabyte
     *
     * @see https://docs.subsquid.io/basics/store/file-store/overview/#filesystem-syncs-and-dataset-partitioning
     *
     * @default 20
     */
    chunkSizeMb?: number

    /**
     * If set, the Database will record a dataset partition
     * upon reaching the blockchain head and then at least
     * once every syncIntervalBlocks if any new data is available.
     *
     * If not set, filesystem writes are triggered only by
     * the amount of in-memory data reaching the chunkSizeMb
     * threshold.
     *
     * Useful for squids with low output data rates.
     *
     * Unit: block
     *
     * @see https://docs.subsquid.io/basics/store/file-store/overview/#filesystem-syncs-and-dataset-partitioning
     */
    syncIntervalBlocks?: number

    /**
     * Overrides of the functions that maintain the filesystem record
     * of the highest indexed block.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/overview/#filesystem-syncs-and-dataset-partitioning
     * @see https://github.com/subsquid/squid-file-store/blob/master/test/src/processor.ts
     */
    hooks?: DatabaseHooks<D>
}

type Chunk<T extends Tables> = {
    [k in keyof T]: TableWriter<T[k] extends Table<infer R> ? R : never>
}

type ToStoreWriter<W extends TableWriter<any>> = Pick<W, 'write' | 'writeMany'>

export type Store<T extends Tables> = Readonly<{
    [k in keyof T]: ToStoreWriter<Chunk<T>[k]>
}>

interface StoreConstructor<T extends Tables> {
    new (chunk: () => Chunk<T>): Store<T>
}

/**
 * Database interface implementation for storing squid data
 * to filesystems.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/
 */
export class Database<T extends Tables, D extends Dest> {
    protected tables: T
    protected dest: D

    protected chunkSize: number
    protected updateInterval: number

    protected chunk?: Chunk<T>
    protected lastCommited?: number

    protected hooks: DatabaseHooks<D>

    protected StoreConstructor: StoreConstructor<T>

    /**
     * Database interface implementation for storing squid data
     * to filesystems.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/
     *
     * @param options - a DatabaseOptions object
     */
    constructor(options: DatabaseOptions<T, D>) {
        this.tables = options.tables
        this.dest = options.dest
        this.chunkSize = options?.chunkSizeMb && options.chunkSizeMb > 0 ? options.chunkSizeMb : 20
        this.updateInterval =
            options?.syncIntervalBlocks && options.syncIntervalBlocks > 0 ? options.syncIntervalBlocks : Infinity
        this.hooks = options.hooks || defaultHooks

        class Store {
            constructor(protected chunk: () => Chunk<T>) {}
        }
        for (let name in this.tables) {
            Object.defineProperty(Store.prototype, name, {
                get(this: Store) {
                    return this.chunk()[name]
                },
            })
        }
        this.StoreConstructor = Store as any
    }

    async connect(): Promise<number> {
        this.lastCommited = await this.hooks.onConnect(this.dest)
        this.chunk = this.chunk || this.createChunk()

        let names = await this.dest.readdir('./')
        for (let name of names) {
            if (!/^(\d+)-(\d+)$/.test(name)) continue

            let chunkStart = Number(name.split('-')[0])
            if (chunkStart > this.lastCommited) {
                await this.dest.rm(name)
            }
        }

        return this.lastCommited
    }

    async close(): Promise<void> {
        this.chunk = undefined
        this.lastCommited = undefined
    }

    async transact(from: number, to: number, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let open = true
        let chunk = this.chunk || this.createChunk()
        let store = new this.StoreConstructor(() => {
            assert(open, `Transaction was already closed`)
            return chunk
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
        assert(this.lastCommited != null, `Not connected to database`)

        if (this.chunk == null) return

        let chunkSize = 0
        for (let name in this.chunk) {
            chunkSize += this.chunk[name].size
        }

        let from = this.lastCommited + 1
        let to = height

        if (
            chunkSize >= this.chunkSize * 1024 * 1024 ||
            (isHead && height - this.lastCommited >= this.updateInterval)
        ) {
            let folderName = from.toString().padStart(10, '0') + '-' + to.toString().padStart(10, '0')
            let chunk = this.chunk
            await this.dest.transact(folderName, async (txDest) => {
                for (let tableAlias in this.tables) {
                    await txDest.writeFile(`${this.tables[tableAlias].name}`, chunk[tableAlias].flush())
                }
            })
            await this.hooks.onFlush(this.dest, {from, to}, isHead ?? false)

            this.lastCommited = height
        }
    }

    private createChunk(): Chunk<T> {
        this.chunk = {} as Chunk<T>
        for (let name in this.tables) {
            this.chunk[name] = this.tables[name].createWriter()
        }
        return this.chunk
    }
}

let DEFAULT_STATUS_FILE = `status.txt`

const defaultHooks: DatabaseHooks = {
    async onConnect(dest) {
        if (await dest.exists(DEFAULT_STATUS_FILE)) {
            let height = await dest.readFile(DEFAULT_STATUS_FILE).then(Number)
            assert(Number.isSafeInteger(height))
            return height
        } else {
            return -1
        }
    },
    async onFlush(dest, range) {
        await dest.writeFile(DEFAULT_STATUS_FILE, range.to.toString())
    },
}
