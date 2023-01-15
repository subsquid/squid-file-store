import {types, JSONType, CsvTable, Column} from '@subsquid/bigdata-csv'
import {TableRecord} from '@subsquid/bigdata-table'
import {rmSync} from 'fs'
import {CsvDatabase} from '../database'

describe('CSV', function () {
    it('output', async function () {
        let db = initDatabase()

        await db.connect()
        await db.transact(0, 0, async (store) => {
            store.write(table, [record1, record2])
        })
        await db.advance(1, true)
        await db.close()
    })
})

export function initDatabase() {
    rmSync('./src/test/data', {force: true, recursive: true})

    return new CsvDatabase({
        tables: [table],
        dest: './src/test/data',
        syncIntervalBlocks: 1,
    })
}

export let table = new CsvTable('test', {
    string: Column(types.string),
    int: Column(types.number),
    float: Column(types.number),
    bigint: Column(types.bigint),
    boolean: Column(types.boolean),
    timestamp: Column(types.timestamp),
    nullableString: Column(types.string, {nullable: true}),
    list: Column(JSONType<string[]>()),
    nullableList: Column(JSONType<(string | null)[]>()),
})

type Record = TableRecord<typeof table>

export let record1: Record = {
    string: 'string',
    int: 3,
    bigint: 4n,
    float: 0.1,
    boolean: true,
    timestamp: new Date(),
    nullableString: null,
    list: ['a', 'b', 'c'],
    nullableList: ['a', null, 'c'],
}

export let record2: Record = {
    string: 'string',
    int: 34684631,
    bigint: 448468676564n,
    float: 0.1111111111111,
    boolean: true,
    timestamp: new Date(),
    nullableString: null,
    list: ['{}', ',', '"'],
    nullableList: [null, null, null],
}
