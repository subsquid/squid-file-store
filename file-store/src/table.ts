export interface TableWriter<T> {
    readonly size: number
    write(record: T): TableWriter<T>
    writeMany(records: T[]): TableWriter<T>
    flush(): Uint8Array
}

export interface Table<T> {
    readonly name: string
    createWriter(): TableWriter<T>
}

export type TableRecord<T extends Table<any>> = T extends Table<infer R> ? R : never
