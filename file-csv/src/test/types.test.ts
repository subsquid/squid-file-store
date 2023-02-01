import expect from 'expect'
import {Column, Table} from '../table'
import * as Types from '../types'
import {readFileSync} from 'fs'
import {TableRecord} from '@subsquid/file-store'

describe('Types', function () {
    it('output', async function () {
        let table = new Table(
            'test',
            {
                string: Column(Types.String()),
                int: Column(Types.Integer()),
                bigint: Column(Types.Integer()),
                decimal: Column(Types.Decimal()),
                boolean: Column(Types.Boolean()),
                timestamp: Column(Types.DateTime()),
                nullableString: Column(Types.String(), {nullable: true}),
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

        let builder = table.createWriter()
        builder.writeMany([record1, record2])
        let result = builder.flush()

        let expected = readFileSync('./src/test/data/types.csv', 'utf-8')
        expect(result).toEqual(expected)
    })
})
