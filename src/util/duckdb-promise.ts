import * as duckdb from 'duckdb'
import {promisify} from 'util'

function wrapMethod<T extends object, R>(methodFn: (...args: any[]) => any): (target: T, ...args: any[]) => Promise<R> {
    return promisify((target: T, ...args: any[]): any => methodFn.bind(target)(...args)) as any
}

const conAll = wrapMethod<duckdb.Connection, duckdb.TableData>(duckdb.Connection.prototype.all)
const conExec = wrapMethod<duckdb.Connection, void>(duckdb.Connection.prototype.exec)
const conPrepare = wrapMethod<duckdb.Connection, duckdb.Statement>(duckdb.Connection.prototype.prepare)
const conRun = wrapMethod<duckdb.Connection, duckdb.Statement>(duckdb.Connection.prototype.run)

export class Connection {
    private con: Promise<duckdb.Connection>

    constructor(ddb: duckdb.Database) {
        this.con = new Promise((resolve, reject) => {
            let dcon = new duckdb.Connection(ddb, (err) => {
                if (err) {
                    reject(err)
                }
                resolve(dcon)
            })
        })
    }

    async all(sql: string, params: any[] = []): Promise<duckdb.TableData> {
        return this.con.then((con) => conAll(con, sql, ...params))
    }

    async exec(sql: string, params: any[] = []): Promise<void> {
        return this.con.then((con) => conExec(con, sql, ...params))
    }

    async prepare(sql: string, params: any[] = []): Promise<Statement> {
        return this.con.then((con) => conPrepare(con, sql, ...params)).then((st) => new Statement(st))
    }

    async run(sql: string, params: any[] = []): Promise<Statement> {
        return this.con.then((con) => conRun(con, sql, ...params))
        .then((st) => new Statement(st))
    }
}

const dbClose = wrapMethod<duckdb.Database, void>(duckdb.Database.prototype.close)
const dbAll = wrapMethod<duckdb.Database, duckdb.TableData>(duckdb.Database.prototype.all)
const dbExec = wrapMethod<duckdb.Database, void>(duckdb.Database.prototype.exec)
const dbPrepare = wrapMethod<duckdb.Database, duckdb.Statement>(duckdb.Database.prototype.prepare)
const dbRun = wrapMethod<duckdb.Database, duckdb.Statement>(duckdb.Database.prototype.run)

export class Database {
    private db: Promise<duckdb.Database>

    constructor(path: string, accessMode?: number) {
        this.db = new Promise((resolve, reject) => {
            let ddb = new duckdb.Database(path, accessMode ?? duckdb.OPEN_READWRITE, (err) => {
                if (err) {
                    reject(err)
                }
                resolve(ddb)
            })
        })
    }

    async close(): Promise<void> {
        return this.db.then((db) => dbClose(db))
    }

    async connect(): Promise<Connection> {
        return this.db.then((db) => new Connection(db))
    }

    async all(sql: string, params: any[] = []): Promise<duckdb.TableData> {
        return this.db.then((db) => dbAll(db, sql, ...params))
    }

    async exec(sql: string, params: any[] = []): Promise<void> {
        return this.db.then((db) => dbExec(db, sql, ...params))
    }

    async prepare(sql: string, params: any[] = []): Promise<Statement> {
        return this.db.then((db) => dbPrepare(db, sql, ...params)).then((st) => new Statement(st))
    }

    async run(sql: string, params: any[] = []): Promise<Statement> {
        return this.db.then((db) => dbRun(db, sql, ...params)).then((st) => new Statement(st))
    }
}

const stRun = wrapMethod<duckdb.Statement, void>(duckdb.Statement.prototype.run)
const stFinalize = wrapMethod<duckdb.Statement, void>(duckdb.Statement.prototype.finalize)
const stAll = wrapMethod<duckdb.Statement, duckdb.TableData>(duckdb.Statement.prototype.all)

export class Statement {
    constructor(private st: duckdb.Statement) {
        this.st = st
    }

    async all(params: any[]): Promise<duckdb.TableData> {
        return stAll(this.st, ...params)
    }

    async run(params: any[]): Promise<Statement> {
        await stRun(this.st, ...params)
        return this
    }

    async finalize(): Promise<void> {
        return stFinalize(this.st)
    }
}

export {
    DuckDbError,
    QueryResult,
    RowData,
    TableData,
    OPEN_CREATE,
    OPEN_FULLMUTEX,
    OPEN_PRIVATECACHE,
    OPEN_READONLY,
    OPEN_READWRITE,
    OPEN_SHAREDCACHE,
} from 'duckdb'
