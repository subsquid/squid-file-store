export interface TableWriter<T> {
    readonly size: number
    write(record: T): TableWriter<T>
    writeMany(records: T[]): TableWriter<T>
    flush(): Uint8Array
}

export interface Table<T> {
    readonly fileName: string
    createWriter(): TableWriter<T>
}

type Simplify<T> = {
    [K in keyof T]: T[K]
} & {}

export type TableRecord<T> = T extends Table<infer R> ? Simplify<R> : never
