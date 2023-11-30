import {Database, LocalDest} from '@subsquid/file-store'
import {Transfers as ParquetTransfers} from './parquet'
import {Transfers as CsvTransafers} from './csv'
import {EvmBatchProcessor} from '@subsquid/evm-processor'
import * as erc20 from './abi/erc20'

const processor = new EvmBatchProcessor()
    .setDataSource({
        archive: 'https://v2.archive.subsquid.io/network/ethereum-mainnet',
        chain: 'https://rpc.ankr.com/eth',
    })
    .setFields({
        log: {
            topics: true,
            data: true,
        },
        transaction: {
            hash: true,
        },
    })
    .setFinalityConfirmation(10)
    .addLog({
        range: {from: 6_082_465},
        address: ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
        topic0: [erc20.events.Transfer.topic],
        transaction: true,
    })

let db = new Database({
    tables: {
        ParquetTransfers,
        CsvTransafers,
    },
    dest: new LocalDest(`./data`),
    chunkSizeMb: 100000000,
    syncIntervalBlocks: 1_000,
    hooks: {
        async onStateRead(fs) {
            if (await fs.exists('./status.json')) {
                let status = await fs.readFile('./status.json').then(JSON.parse)
                return status
            } else {
                return undefined
            }
        },
        async onStateUpdate(fs, info) {
            await fs.writeFile('./status.json', JSON.stringify(info))
        },
    },
})

let forced = false

processor.run(db, async (ctx) => {
    for (let block of ctx.blocks) {
        for (let log of block.logs) {
            if (log.topics[0] === erc20.events.Transfer.topic) {
                let event = erc20.events.Transfer.decode(log)
                let transfer = {
                    blockNumber: block.header.height,
                    timestamp: new Date(block.header.timestamp),
                    from: event.from,
                    to: event.to,
                    amount: event.value,
                }
                ctx.store.CsvTransafers.write(transfer)
                ctx.store.ParquetTransfers.write(transfer)
            }
        }
    }

    ctx.store.setForceFlush((forced = !forced))
})
