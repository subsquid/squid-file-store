import {assertNotNull} from '@subsquid/util-internal'
import {TableBuilder, TableHeader} from './table'

export class Chunk {
    constructor(private from: number, private to: number, private tables: Map<string, TableBuilder<any>>) {}

    getTableBuilder<T extends TableHeader>(name: string): TableBuilder<T> {
        return assertNotNull(this.tables.get(name), `Table ${name} does not exist`)
    }

    changeRange(range: {from?: number; to?: number}) {
        if (range.from) this.from = range.from
        if (range.to) this.to = range.to
    }

    getSize(encoding: BufferEncoding) {
        let total = 0
        for (let table of this.tables.values()) {
            total += table.getSize(encoding)
        }
        return total
    }

    get name() {
        return `${this.from.toString().padStart(10, '0')}-${this.to.toString().padStart(10, '0')}`
    }
}
