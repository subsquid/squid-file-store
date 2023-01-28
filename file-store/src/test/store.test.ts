import {rmSync} from 'fs'
import {Database} from '../database'
import {Table} from '../table'

describe('Store', function () {
    it('output', async function () {
        let db = initDatabase()

        await db.connect()
        await db.transact(0, 0, async (store) => {
            store.tables.foo.write({
                a: 'hello',
                b: 10,
            })
            store.tables.bar.write({
                a: 10,
                b: 'hello',
            })
        })
        await db.advance(1, true)
        await db.close()
    })
})

export function initDatabase() {
    rmSync('./src/test/data', {force: true, recursive: true})

    return new Database({
        tables: {
            foo: {} as Table<{a: string; b: number}>,
            bar: {} as Table<{a: number; b: string}>,
        },
        dest: './src/test/data',
        syncIntervalBlocks: 1,
    })
}
