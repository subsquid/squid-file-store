import {BigDecimal} from '@subsquid/big-decimal'
import {CsvDatabase} from '../database'
import {Table, TableRecord} from '../table'
import {List, Struct, types} from '../table'

let table = new Table('csv-test', {
    a: types.string,
    b: types.int,
    c: types.float,
    d: types.boolean,
    e: types.bigint,
    f: {type: types.string, nullable: true},
    g: List(types.string),
    h: Struct({
        a: types.string,
        b: {type: types.boolean, nullable: true},
    }),
    j: {type: types.bigdecimal, nullable: false},
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
    j: BigDecimal('0.1'),
    k: [1, 1],
}

let record2: Record = {
    a: 'a',
    b: 1,
    c: 0.1,
    d: true,
    e: 1n,
    g: ['g'],
    h: {a: 'a', b: false},
    j: BigDecimal('0.1'),
    k: [1, null],
}

async function test() {
    let db = new CsvDatabase([table], {dest: './src/test/data'})
    await db.connect()
    await db.transact(0, 0, async (store) => {
        await store.write(table, [record1, record2])
    })
    await db.outputTables('./')
}

test()
