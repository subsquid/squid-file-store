/**
 * Abstrace interface for objects that buffer tabular data and convert it
 * into format-specific file contents. An implementation is available for
 * every implementation of Table.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/
 */
export interface TableWriter<T> {
    readonly size: number

    /**
      * Stores a single row of data into an in-memory buffer.
      *
      * @param record - a mapping from column names to data values
      *
      * @returns this - this is a chainable function
      */
    write(record: T): TableWriter<T>

    /**
      * Stores multiple rows of data into an in-memory buffer.
      *
      * @param records - an array of mappings from column names to data values
      *
      * @returns this - this is a chainable function
      */
    writeMany(records: T[]): TableWriter<T>
    flush(): Uint8Array
}

/**
 * Interface for objects that make TableWriters: objects that
 * buffer tabular data and convert it into format-specific file contents.
 *
 * For available implementations
 * @see https://docs.subsquid.io/basics/store/file-store/
 */
export interface Table<T> {
    readonly name: string
    createWriter(): TableWriter<T>
}

type Simplify<T> = {
    [K in keyof T]: T[K]
} & {}

/**
 * Interface for table rows used by Table and TableWriter objects.
 */
export type TableRecord<T> = T extends Table<infer R> ? Simplify<R> : never
