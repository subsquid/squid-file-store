import assert from 'assert'
import {Table, TableWriter} from './table'
import {Dest, LocalDest} from './dest'

interface DatabaseHooks<D extends Dest = Dest> {
    onConnect(dest: D): Promise<number>
    onFlush(dest: D, height: number, isHead: boolean): Promise<void>
}

type Tables = Record<string, Table<any>>

export interface DatabaseOptions<T extends Tables, D extends Dest> {
    tables: T

    dest: D
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

export class Database<T extends Tables, D extends Dest> {
    protected tables: T
    protected dest: D

    protected chunkSize: number
    protected updateInterval: number

    protected chunk?: Chunk<T>
    protected lastCommited?: number

    protected hooks: DatabaseHooks<D>

    protected StoreConstructor: StoreConstructor<T>

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

        this.chunk = chunk

        open = false
    }

    async advance(height: number, isHead?: boolean): Promise<void> {
        assert(this.lastCommited != null, `Not connected to database`)

        if (this.chunk == null) return
        let chunk = this.chunk

        let chunkSize = 0
        for (let name in chunk) {
            chunkSize += chunk[name].size
        }

        let from = this.lastCommited + 1
        let to = height

        if (
            chunkSize >= this.chunkSize * 1024 * 1024 ||
            (isHead && height - this.lastCommited >= this.updateInterval && chunkSize > 0)
        ) {
            let folderName = from.toString().padStart(10, '0') + '-' + to.toString().padStart(10, '0')
            await this.dest.transact(folderName, async (txDest) => {
                for (let name in this.tables) {
                    await txDest.writeFile(`${this.tables[name].name}`, chunk[name].flush())
                }
            })
            await this.hooks.onFlush(this.dest, height, isHead ?? false)

            this.lastCommited = height
            this.chunk = undefined
        }
    }

    private createChunk(): Chunk<T> {
        let chunk: Chunk<T> = {} as any
        for (let name in this.tables) {
            chunk[name] = this.tables[name].createWriter()
        }
        return chunk
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
    async onFlush(dest, height) {
        await dest.writeFile(DEFAULT_STATUS_FILE, height.toString())
    },
}
