import expect from 'expect'
import {TableRecord} from '@subsquid/file-table'
import {Column, Table} from '../table'
import {BooleanType, DateTimeType, DecimalType, IntegerType, StringType} from '../types'
import {readFileSync} from 'fs'

describe('Types', function () {
    it('output', async function () {
        let builder = table.createTableBuilder()
        builder.append(record1)
        builder.append(record2)
        let result = builder.flush()

        let expected = readFileSync('./src/test/data/types.csv', 'utf-8')
        expect(result).toEqual(expected)
    })
})

let table = new Table(
    'test',
    {
        string: Column(StringType()),
        int: Column(IntegerType()),
        bigint: Column(IntegerType()),
        decimal: Column(DecimalType()),
        boolean: Column(BooleanType()),
        timestamp: Column(DateTimeType()),
        nullableString: Column(StringType(), {nullable: true}),
    },
    {
        header: false,
    }
)

type Record = TableRecord<typeof table>

let record1: Record = {
    string: 'string',
    int: 3,
    bigint: 4n,
    decimal: 0.1,
    boolean: true,
    timestamp: new Date(1674214502006),
    nullableString: null,
}

let record2: Record = {
    string: 'string',
    int: 34684631,
    bigint: 448468676564n,
    decimal: 0.1111111111111,
    boolean: true,
    timestamp: new Date(1575583206000),
    nullableString: 'not null',
}
