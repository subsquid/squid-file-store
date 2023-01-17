import {
    Table,
    Column,
    StringType,
    Int32Type,
    Int64Type,
    FloatType,
    TimestampType,
    BooleanType,
    DateType,
    ListType,
} from '@subsquid/bigdata-parquet'
import {TableRecord} from '@subsquid/bigdata-table'
import {rmSync} from 'fs'
import {CsvDatabase} from '../database'

describe('Parquet', function () {
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
    rmSync('./src/test/data/parquet', {force: true, recursive: true})

    return new CsvDatabase({
        tables: [table],
        dest: './src/test/data/parquet',
        syncIntervalBlocks: 1,
    })
}

export let table = new Table('test', {
    string: Column(StringType()),
    int: Column(Int32Type()),
    bigint: Column(Int64Type()),
    float: Column(FloatType()),
    boolean: Column(BooleanType()),
    timestamp: Column(TimestampType()),
    date: Column(DateType()),
    nullableString: Column(StringType(), {nullable: true}),
    list: Column(ListType(Int32Type())),
    nullableList: Column(ListType(Int32Type(), {nullable: true})),
})

type Record = TableRecord<typeof table>

export let record1: Record = {
    string: 'hello',
    int: 3,
    bigint: 4n,
    float: 0.1,
    boolean: true,
    timestamp: new Date(),
    date: new Date(),
    nullableString: 'hello',
    list: [1, 2, 3, 4],
    nullableList: [null, 3, 4],
}

export let record2: Record = {
    string: 'string',
    int: 34684631,
    bigint: 448468676564n,
    float: 0.1111111111111,
    boolean: true,
    timestamp: new Date(),
    date: new Date(),
    nullableString: null,
    list: [],
    nullableList: [null, null, null],
}
