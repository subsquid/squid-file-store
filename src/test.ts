import * as duckdb from './util/duckdb-promise'
import {promisify} from 'util'

let a = async () => {
    let db = new duckdb.Database(':memory:')
    let con = await db.connect()
    await con.run(`CREATE TABLE people(id INTEGER, name VARCHAR)`)
    await con.run(`INSERT INTO people VALUES (1, 'AAA'), (2, 'AAA')`)
    await con.all(`COPY people FROM './test.parquet' (FORMAT PARQUET)`)
}

a()
