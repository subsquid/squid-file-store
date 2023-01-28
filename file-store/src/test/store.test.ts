import {TableRecord} from '@subsquid/file-table'
import {rmSync} from 'fs'
import {Database} from '../database'
import {CsvTable} from './csv.table'
import {ParquetTable} from './parquet.table'

describe('Store', function () {
    it('output', async function () {
        let db = initDatabase()

        await db.connect()
        await db.transact(0, 0, async (store) => {
            store.write(CsvTable, {
                blockNumber: 5089937,
                timestamp: new Date(`2020-11-27T06:47:12.000Z`),
                extrinsicHash: `0x77709e82369fc279c64aeb44c7ec7b05362a3599f0e27b62f6c5a19e3159b6d1`,
                from: `EEsp9Duot6U3F1fgA2MXBHeArtkySqpXWYkcBj7wfjC6rLY`,
                to: `Cb2QccEAM38pjwmcHHTTuTukUobTHwhakKH4kBo4k8Vur8o`,
                amount: 997433334294n,
            })
            store.write(ParquetTable, {
                blockNumber: 5089937,
                timestamp: new Date(`2020-11-27T06:47:12.000Z`),
                extrinsicHash: `0x77709e82369fc279c64aeb44c7ec7b05362a3599f0e27b62f6c5a19e3159b6d1`,
                from: `EEsp9Duot6U3F1fgA2MXBHeArtkySqpXWYkcBj7wfjC6rLY`,
                to: `Cb2QccEAM38pjwmcHHTTuTukUobTHwhakKH4kBo4k8Vur8o`,
                amount: 997433334294n,
            })
        })
        await db.advance(1, true)
        await db.close()
    })
})

export function initDatabase() {
    rmSync('./src/test/data', {force: true, recursive: true})

    return new Database({
        tables: [CsvTable, ParquetTable],
        dest: './src/test/data',
        syncIntervalBlocks: 1,
    })
}
