import {BigDecimal} from '@subsquid/big-decimal'
import {CsvDatabase, ParquetDatabase} from '../database'
import {Table, TableRecord} from '../table'
import {List, Struct, types} from '../table'

describe('Store', function () {
    let table = new Table('test', {
        a: types.string,
        b: types.int,
        c: types.float,
        d: types.boolean,
        e: types.bigInt,
        f: {type: types.string, nullable: true},
        g: List(types.string),
        h: Struct({
            a: types.string,
            b: {type: types.boolean, nullable: true},
        }),
        j: {type: types.smallInt, nullable: false},
        k: List(types.int, {nullable: true}),
    })

    type Record = TableRecord<typeof table>

    let record1: Record = {
        a: 'a',
        b: 1,
        c: 0.1,
        d: true,
        e: 1n,
        f: 'f',
        g: ['g'],
        h: {a: 'a'},
        j: 4,
        k: [1, 1],
    }

    let record2: Record = {
        a: 'asdfassad',
        b: 1000000,
        c: 0.0000005,
        d: true,
        e: 1000000000000n,
        g: [],
        h: {a: 'a', b: false},
        j: -5,
        k: [1, null, 54687664],
    }

    it('csv', async function () {
        let db = new CsvDatabase([table], {dest: './src/test/data'})
        await db.connect()
        await db.transact(0, 0, async (store) => {
            await store.write(table, [record1, record2])
        })
        await db.outputTables('./')
        await db.close()
    })

    it('parquet', async function () {
        let db = new ParquetDatabase([table], {dest: './src/test/data'})
        await db.connect()
        await db.transact(0, 0, async (store) => {
            await store.write(table, [record1, record2])
        })
        await db.outputTables('./')
        await db.close()
    })
})
