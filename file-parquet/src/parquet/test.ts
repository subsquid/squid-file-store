import {ParquetSchema} from './schema'
import {ParquetEnvelopeWriter, ParquetWriter} from './writer'
import {writeFileSync} from 'fs'

let schema = new ParquetSchema({
    name: {type: 'UTF8'},
    quantity: {type: 'INT64', optional: true},
    price: {type: 'DOUBLE'},
})

let res: Buffer[] = []

let write = new ParquetWriter(schema, new ParquetEnvelopeWriter(schema, 0))

function test() {
    write.appendRow({
        name: 'apple',
        quantity: 10,
        price: 23.5,
    })
    write.appendRow({
        name: 'apple',
        quantity: 10,
        price: 23.5,
    })
    write.appendRow({
        name: 'apple',
        quantity: 10,
        price: 23.5,
    })
    write.appendRow({
        name: 'apple',
        quantity: 10,
        price: 23.5,
    })
    write.appendRow({
        name: 'apple',
        quantity: 10,
        price: 23.5,
    })
    write.appendRow({
        name: 'apple',
        quantity: 10,
        price: 23.5,
    })

    let table = write.close()
    writeFileSync('test.parquet', table)
}

test()
