import {rmSync} from 'fs'
import {CsvDatabase} from '../database'
import {table, record1, record2} from './table'

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
    rmSync('./src/test/data/status.json', {force: true})

    return new CsvDatabase([table], {dest: './src/test/data', updateInterval: 1})
}
