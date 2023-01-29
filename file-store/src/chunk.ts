import {TableWriter, Table} from './table'

export class Chunk {
    readonly writers: Record<string, TableWriter<any>> = {}

    constructor(public from: number, public to: number, tables: Record<string, Table<any>>) {
        for (let name in tables) {
            this.writers[name] = tables[name].createWriter()
        }
    }

    get size() {
        let total = 0
        for (let name in this.writers) {
            total += this.writers[name].size
        }
        return total
    }
}
