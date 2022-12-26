import {assertNotNull} from '@subsquid/util-internal'
import assert from 'assert'
import {Dialect, dialects} from './util/dialect'
import {createFS, FS, S3Fs, S3Options} from './util/fs'
import {Table, TableHeader, TableRecord} from './table'
import * as duckdb from './util/duckdb-promise'
import {Type} from './table'

const PENDING_FOLDER = 'last'
const STATUS_TABLE = 'status'

interface DatabaseOptions {
    /**
     * Local or s3 destination.
     * For s3 use 's3://*bucketName/path'
     * @Default ./data
     */
    dest?: string
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
    s3Options?: S3Options
}

interface OutputOptions {
    extension?: string
}

abstract class BaseDatabase {
    protected chunkSize: number
    protected updateInterval: number
    protected s3Options?: S3Options
    protected outputOptions: OutputOptions

    protected lastUpdated = -1

    protected fs: FS
    protected _con?: duckdb.Database

    protected get con() {
        return assertNotNull(this._con, 'Not connected to database')
    }

    constructor(private tables: Table<any>[], options?: CsvDatabaseOptions) {
        this.chunkSize = options?.chunkSize || 20
        this.updateInterval = options?.updateInterval && options.updateInterval > 0 ? options.updateInterval : Infinity
        this.s3Options = options?.s3Options
        this.outputOptions = {extension: ''}
        this.fs = createFS(options?.dest || './data', options?.s3Options)
    }

    async connect(): Promise<number> {
        await this.fs.init()
        await this.fs.remove(PENDING_FOLDER)

        this._con = new duckdb.Database(':memory:')

        if (this.fs instanceof S3Fs) await this.setupS3()

        await this.createTables()
        await this.con.run(`CREATE SCHEMA squid`)
        await this.con.run(`CREATE TABLE squid.status("height" UINTEGER, "chunks" STRING[])`)
        await this.con.run(
            `CREATE TABLE squid.chunk("from" UINTEGER NOT NULL, "to" UINTEGER NOT NULL, "size" UINTEGER NOT NULL)`
        )

        if (await this.fs.exist(`${STATUS_TABLE}.${this.outputOptions.extension}`)) {
            try {
                await this.con.run(
                    `COPY squid.status FROM '${this.fs.abs(
                        `${STATUS_TABLE}.${this.outputOptions.extension}`
                    )}' WITH (${this.getOutputOptions().join(', ')})`
                )
                let status = await this.con.all(`SELECT height FROM squid.status`).then((r) => assertNotNull(r[0]))
                this.lastUpdated = status.height
            } catch (e: any) {
                await this.con.close()
                throw e
            }
        } else {
            await this.con.run(`INSERT INTO squid.status VALUES (?, ?)`, [null, '[]'])
            this.lastUpdated = -1
        }

        return this.lastUpdated
    }

    async close(): Promise<void> {
        await this.con.close()
        this._con = undefined
    }

    async transact(from: number, to: number, cb: (store: Store) => Promise<void>): Promise<void> {
        let retries = 3
        while (true) {
            try {
                let open = true

                let chunkNotExist = await this.con.all(`UPDATE squid.chunk SET "to"=${to}`).then((r) => !r[0]?.Count)
                if (chunkNotExist) {
                    await this.con.run(`INSERT INTO squid.chunk VALUES (?, ?, ?)`, [from, to, 0])
                }

                let store = new Store(() => {
                    assert(open, `Transaction was already closed`)
                    return this.con
                })

                try {
                    await cb(store)
                } catch (e: any) {
                    open = false
                    throw e
                }

                open = false
                break
            } catch (e: any) {
                if (retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    async advance(height: number): Promise<void> {
        let chunk = await this.con
            .all(`SELECT * FROM squid.chunk`)
            .then((r) => r[0] as {from: number; to: number; size: number})
        if (chunk == null) return

        if (chunk.to < height) {
            chunk.to = height
        }

        if (chunk.size >= this.chunkSize * 1024 * 1024) {
            await this.fs.remove(PENDING_FOLDER)

            let folderName = `${chunk.from.toString().padStart(10, '0')}-${chunk.to.toString().padStart(10, '0')}`
            await this.outputTables(folderName)
            await this.clearTables()
            await this.con.run(`DELETE FROM squid.chunk`)
            await this.con.run(
                `UPDATE squid.status SET height=${chunk.to}, chunks=array_append(chunks, '${folderName}')`
            )
            await this.con.run(
                `COPY squid.status TO '${this.fs.abs(
                    `${STATUS_TABLE}.${this.outputOptions.extension}`
                )}'  WITH (HEADER true)`
            )
            this.lastUpdated = height
        } else if (height - this.lastUpdated >= this.updateInterval) {
            await this.outputTables(PENDING_FOLDER)
            this.lastUpdated = height
        }
    }

    private async createTables() {
        for (let table of this.tables) {
            let fields: [string, Type<any>][] = Object.entries(table.header)
            await this.con.run(`CREATE TABLE "${table.name}"(${table.serializeFieldTypes()})`)
        }
    }

    async clearTables() {
        for (let table of this.tables) {
            await this.con.run(`DELETE FROM "${table.name}"`)
        }
    }

    async outputTables(path: string) {
        await this.fs.mkdir(path)

        for (let table of this.tables) {
            await this.con.run(
                `COPY "${table.name}" TO '${this.fs.abs(
                    path,
                    `${table.name}.${this.outputOptions.extension}`
                )}' WITH (${this.getOutputOptions().join(', ')})`
            )
        }
    }

    private async setupS3() {
        let s3Options = assertNotNull(this.s3Options)
        await this.con.run(`INSTALL httpfs`)
        await this.con.run(`LOAD httpfs`)
        await this.con.run(`SET s3_access_key_id='${s3Options.accessKeyId}'`)
        await this.con.run(`SET s3_secret_access_key='${s3Options.secretAccessKey}'`)
        if (s3Options.region) await this.con.run(`SET s3_region='${s3Options.region}'`)
        if (s3Options.endpoint)
            await this.con.run(`SET s3_endpoint='${s3Options.endpoint.replace(/(^\w+:|^)\/\//, '')}'`)
        if (s3Options.sessionToken) await this.con.run(`SET s3_session_token='${s3Options.sessionToken}'`)
    }

    protected getOutputOptions(): string[] {
        return []
    }
}

interface CsvOutputOptions {
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
     * @Default true
     */
    header?: boolean
}

interface CsvDatabaseOptions extends DatabaseOptions {
    outputOptions?: CsvOutputOptions
}

export class CsvDatabase extends BaseDatabase {
    protected outputOptions: Required<CsvOutputOptions>

    constructor(tables: Table<any>[], options?: CsvDatabaseOptions) {
        super(tables, options)
        this.outputOptions = {extension: 'csv', header: true, dialect: dialects.excel, ...options?.outputOptions}
    }

    protected getOutputOptions() {
        let res: string[] = []
        res.push(`FORMAT 'CSV'`)
        res.push(`HEADER ${this.outputOptions.header}`)
        res.push(`DELIMITER  '${this.outputOptions.dialect.delimiter}'`)
        res.push(`QUOTE  '${this.outputOptions.dialect.quoteChar}'`)
        if (this.outputOptions.dialect.escapeChar != null) {
            res.push(`ESCAPE  '${this.outputOptions.dialect.escapeChar}'`)
        }
        return res
    }
}

export enum Compression {
    None = 'UNCOMPRESSED',
    Zstd = 'ZSTD',
    Snappy = 'SNAPPY',
    Gzip = 'GZIP',
}

interface ParquetOutputOptions {
    /**
     * Output files extension.
     * @Default csv
     */
    extension?: string

    compression?: Compression
}

interface ParquetDatabaseOptions extends DatabaseOptions {
    outputOptions?: ParquetOutputOptions
}

export class ParquetDatabase extends BaseDatabase {
    protected outputOptions: Required<ParquetOutputOptions>

    constructor(tables: Table<any>[], options?: ParquetDatabaseOptions) {
        super(tables, options)
        this.outputOptions = {extension: 'parquet', compression: Compression.None, ...options?.outputOptions}
    }

    protected getOutputOptions() {
        let res: string[] = []
        res.push(`FORMAT 'PARQUET'`)
        res.push(`CODEC  '${this.outputOptions.compression}'`)
        return res
    }
}

export class Store {
    constructor(private con: () => duckdb.Database) {}

    async write<T extends TableHeader>(table: Table<T>, records: TableRecord<T> | TableRecord<T>[]): Promise<void> {
        records = Array.isArray(records) ? records : [records]

        let fields = Object.entries(table.header)
        let st = await this.con().prepare(
            `INSERT INTO "${table.name}" VALUES (${new Array(fields.length).fill('?').join(`, `)})`
        )

        let size = 0
        for (let record of records) {
            let values = table.serializeRecord(record)
            await st.run(values)
            size += Buffer.byteLength(values.join())
        }

        await this.con().run(`UPDATE squid.chunk SET size = size + ${size}`)
    }
}
