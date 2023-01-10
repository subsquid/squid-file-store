import {rmSync} from 'fs'
import {CsvDatabase} from '../database'
import {Table, TableRecord, types} from '../table'

async function test() {
    let db = initDatabase()

    await db.connect()
    await db.transact(0, 0, async (store) => {
        for (let i = 0; i < 1_000_000; i++) {
            store.write(Transfers, record)
        }
    })
    await db.advance(1, true)
    await db.close()
}

export function initDatabase() {
    rmSync('./src/test/data', {force: true, recursive: true})

    return new CsvDatabase({
        tables: [Transfers],
        dest: './src/test/data',
        syncIntervalBlocks: 1,
    })
}

export const Transfers = new Table('transfers', {
    blockNumber: types.number,
    timestamp: types.timestamp,
    extrinsicHash: {type: types.string, nullable: true},
    from: types.string,
    to: types.string,
    amount: types.bigint,
})

export let record: TableRecord<typeof Transfers> = {
    blockNumber: 5089937,
    timestamp: new Date(`2020-11-27T06:47:12.000Z`),
    extrinsicHash: `0x77709e82369fc279c64aeb44c7ec7b05362a3599f0e27b62f6c5a19e3159b6d1`,
    from: `EEsp9Duot6U3F1fgA2MXBHeArtkySqpXWYkcBj7wfjC6rLY`,
    to: `Cb2QccEAM38pjwmcHHTTuTukUobTHwhakKH4kBo4k8Vur8o`,
    amount: 997433334294n,
}

test()