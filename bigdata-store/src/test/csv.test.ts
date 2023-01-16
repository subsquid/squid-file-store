import {Table, Column, StringType, IntegerType, DecimalType, DateTimeType, BooleanType} from '@subsquid/bigdata-csv'
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
    rmSync('./src/test/data/csv', {force: true, recursive: true})

    return new CsvDatabase({
        tables: [table],
        dest: './src/test/data/csv',
        syncIntervalBlocks: 1,
    })
}

export let table = new Table('test', {
    string: Column(StringType()),
    int: Column(IntegerType()),
    bigint: Column(IntegerType()),
    decimal: Column(DecimalType()),
    boolean: Column(BooleanType()),
    timestamp: Column(DateTimeType()),
    nullableString: Column(StringType(), {nullable: true}),
})

type Record = TableRecord<typeof table>

export let record1: Record = {
    string: 'string',
    int: 3,
    bigint: 4n,
    decimal: 0.1,
    boolean: true,
    timestamp: new Date(),
    nullableString: null,
}

export let record2: Record = {
    string: 'string',
    int: 34684631,
    bigint: 448468676564n,
    decimal: 0.1111111111111,
    boolean: true,
    timestamp: new Date(),
    nullableString: null,
}
