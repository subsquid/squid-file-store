import {assertNotNull, def} from '@subsquid/util-internal'
import {FinalDatabase, FinalTxInfo, HashAndHeight} from '@subsquid/util-internal-processor-tools'
import assert from 'assert'
import {Dest} from './dest'
import {Table, TableWriter} from './table'
import {createFolderName, isFolderName} from './util'
import {createLogger} from '@subsquid/logger'

export interface DatabaseHooks<D extends Dest = Dest> {
    onStateRead(dest: D): Promise<HashAndHeight | undefined>
    onStateUpdate(dest: D, state: HashAndHeight, prev?: HashAndHeight): Promise<void>
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
     * @deprecated Use {@link Store.setForceFlush} instead
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

type DataBuffer<T extends Tables> = {
    [k in keyof T]: TableWriter<T[k] extends Table<infer R> ? R : never>
}

type ToStoreWriter<W extends TableWriter<any>> = Pick<W, 'write' | 'writeMany'>

abstract class BaseStore<T extends Tables> {
    static create<T extends Tables>(tables: T): StoreConstructor<T> {
        const S = class extends BaseStore<T> {}

        for (let name in tables) {
            Object.defineProperty(S.prototype, name, {
                get(this: InstanceType<typeof S>) {
                    return this.chunk()[name]
                },
            })
        }

        return S as any
    }

    private _isForced = false

    get isForced() {
        return this._isForced
    }

    constructor(protected chunk: () => DataBuffer<T>) {}

    setForceFlush(value: boolean) {
        this._isForced = value
    }
}

export type Store<T extends Tables> = BaseStore<T> &
    Readonly<{
        [k in keyof T]: ToStoreWriter<DataBuffer<T>[k]>
    }>

interface StoreConstructor<T extends Tables> {
    new (chunk: () => DataBuffer<T>): Store<T>
}

/**
 * Database interface implementation for storing squid data
 * to filesystems.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/
 */
export class Database<T extends Tables, D extends Dest> implements FinalDatabase<Store<T>> {
    private tables: T
    private dest: D
    private chunkSize: number
    private hooks: DatabaseHooks<D>

    private StoreConstructor: StoreConstructor<T>

    private chunk?: DataBuffer<T>
    private state?: HashAndHeight

    private isForced = false

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

        this.chunkSize = options?.chunkSizeMb ?? 20
        assert(this.chunkSize > 0, `invalid chunk size ${this.chunkSize}`)

        if (options?.syncIntervalBlocks != null) {
            const log = this.getLogger()
            log.warn(
                '`syncIntervalBlocks` option is deprecated and no longer has any effect. ' +
                    'Please consider using `Store.setForceFlush()` isntead.'
            )
        }

        this.hooks = options.hooks || defaultHooks

        this.StoreConstructor = BaseStore.create(this.tables)
    }

    async connect(): Promise<HashAndHeight> {
        this.state = await this.getState()

        let names = await this.dest.readdir('./')
        for (let name of names) {
            if (!isFolderName(name)) continue

            let chunkStart = Number(name.split('-')[0])
            if (chunkStart > this.state.height) {
                await this.dest.rm(name)
            }
        }

        return this.state
    }

    async transact(info: FinalTxInfo, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let dbState = await this.getState()
        let prevState = assertNotNull(this.state, 'not connected')
        let {nextHead: newState} = info

        assert(
            dbState.hash === prevState.hash && dbState.height === prevState.height,
            'state was updated by foreign process, make sure no other processor is running'
        )
        assert(prevState.height < newState.height)
        assert(prevState.hash != newState.hash)

        this.chunk = this.chunk || this.createChunk()
        await this.performUpdates(cb, this.chunk)

        let chunkSize = 0
        for (let name in this.chunk) {
            chunkSize += this.chunk[name].size
        }

        if (this.isForced || chunkSize >= this.chunkSize * 1024 * 1024) {
            if (chunkSize > 0) {
                await this.flush(prevState, newState, this.chunk)
            }
            await this.hooks.onStateUpdate(this.dest, newState, prevState)
            this.state = newState
            this.isForced = false
        }
    }

    private async flush(prevState: HashAndHeight, newState: HashAndHeight, chunk: DataBuffer<T>) {
        let folderName = createFolderName(prevState.height + 1, newState.height)
        await this.dest.transact(folderName, async (txDest) => {
            for (let tableAlias in this.tables) {
                await txDest.writeFile(`${this.tables[tableAlias].name}`, chunk[tableAlias].flush())
            }
        })
    }

    private async performUpdates(cb: (store: Store<T>) => Promise<void>, chunk: DataBuffer<T>): Promise<void> {
        let running = true
        let store = new this.StoreConstructor(() => {
            assert(running, `too late to perform updates`)
            return chunk
        })

        try {
            await cb(store)
            this.isForced = store.isForced
        } finally {
            running = false
        }
    }

    private async getState(): Promise<HashAndHeight> {
        let state = await this.hooks.onStateRead(this.dest)
        if (state == null) {
            state = {height: -1, hash: '0x'}
            await this.hooks.onStateUpdate(this.dest, state)
        }
        assert(Number.isSafeInteger(state.height))
        return state
    }

    private createChunk(): DataBuffer<T> {
        let chunk = {} as DataBuffer<T>
        for (let name in this.tables) {
            chunk[name] = this.tables[name].createWriter()
        }
        return chunk
    }

    @def
    private getLogger() {
        return createLogger('sqd:file-store')
    }
}

const DEFAULT_STATUS_FILE = `status.txt`
const defaultHooks: DatabaseHooks = {
    async onStateRead(dest) {
        if (await dest.exists(DEFAULT_STATUS_FILE)) {
            let [height, hash] = await dest.readFile(DEFAULT_STATUS_FILE).then((d) => d.split('\n'))
            return {height: Number(height), hash: hash || '0x'}
        } else {
            return undefined
        }
    },
    async onStateUpdate(dest, info) {
        await dest.writeFile(DEFAULT_STATUS_FILE, info.height + '\n' + info.hash)
    },
}
