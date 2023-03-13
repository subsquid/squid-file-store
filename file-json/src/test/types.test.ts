import {Table} from '../table'
import {TableRecord} from '@subsquid/file-store'

describe('Types', function () {
    it('output', async function () {
        let table = new Table<{
            string: string
            numeric: number
            bigint: bigint
            boolean: boolean
            timestamp: Date
            null: null | undefined
        }>('test', {lines: false})

        type Record = TableRecord<typeof table>

        let record1: Record = {
            string: 'string',
            numeric: 3,
            bigint: 4n,
            boolean: true,
            timestamp: new Date(1674214502006),
            null: null,
        }

        let record2: Record = {
            string: 'string',
            numeric: 34684631,
            bigint: 448468676564n,
            boolean: true,
            timestamp: new Date(1575583206000),
            null: undefined,
        }

        let builder = table.createWriter()
        builder.writeMany([record1, record2])
        let result = builder.flush()

        console.log(Buffer.from(result).toString('utf-8'))

        // expect(result).toEqual(expected)
    })
})
