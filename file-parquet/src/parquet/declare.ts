import {LogicalType} from '../../thrift/parquet_types'

export type ParquetCodec = 'PLAIN' | 'RLE'
export type ParquetCompression = 'UNCOMPRESSED' | 'GZIP' | 'LZO' | 'BROTLI' | 'LZ4' // | 'SNAPPY'
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED'
export type ParquetType = PrimitiveType | OriginalType

export type PrimitiveType =
    // Base Types
    | 'BOOLEAN' // 0
    | 'INT32' // 1
    | 'INT64' // 2
    | 'INT96' // 3
    | 'FLOAT' // 4
    | 'DOUBLE' // 5
    | 'BYTE_ARRAY' // 6,
    | 'FIXED_LEN_BYTE_ARRAY' // 7

export type OriginalType =
    // Converted Types
    | 'UTF8' // 0
    // | 'MAP' // 1
    // | 'MAP_KEY_VALUE' // 2
    // | 'LIST' // 3
    // | 'ENUM' // 4
    // | 'DECIMAL' // 5
    | 'DATE' // 6
    | 'TIME_MILLIS' // 7
    | 'TIME_MICROS' // 8
    | 'TIMESTAMP_MILLIS' // 9
    | 'TIMESTAMP_MICROS' // 10
    | 'UINT_8' // 11
    | 'UINT_16' // 12
    | 'UINT_32' // 13
    | 'UINT_64' // 14
    | 'INT_8' // 15
    | 'INT_16' // 16
    | 'INT_32' // 17
    | 'INT_64' // 18
    | 'JSON' // 19
    | 'BSON' // 20
    | 'INTERVAL' // 21

export interface ShrededColumn {
    /**
     * Values read from the column.  May be returned as a primitive
     * array format if it is able to.
     *
     * - UInt8Array: BOOLEAN
     * - Int32Array: INT32
     * - Float32Array: FLOAT
     * - Float64Array: DOUBLE
     */
    values: any[]

    /**
     * For fields which have REPEATED fields in their field path, repetition levels may be specified.
     *
     * The repetition level indicates the number of repeated fields in the field path that are being
     * "repeated" (rather than started anew in their parent record / array / row).
     *
     * The field path refers to the field and its parent fields if it is nested.
     *
     * A repetition level of zero indicates the first value for all the repeated fields in the path;
     * they would all get a fresh new array.
     *
     * When the repetition level is greater than zero and less than the number of repeated fields
     * (rLevelMax) in the field path we can keep that number of parent arrays but create new arrays
     * after that.
     *
     * When the repetition level is equal to the number of repeated fields (rLevelMax) we are simply
     * adding to the end of the most recently created array.
     *
     * If the field path has only one repeated field, the repetition level will be 0 for the first
     * element of a given row and 1 for the ones that follow.
     *
     * If the field path has two repeated fields, the repetition level will be 0 for the first element
     * in a row, 1 for the first element of the second set of values for the first repeated field in
     * the field path, and 2 for subsequent elements in a single group/array.
     */
    rLevels: number[]

    /**
     * Definition levels specify how many optional fields in the path for the column are defined.
     *
     * If the definition level for a row is less than the number of optional fields in field path
     * (dLevelMax), there will not be a value or repetition level encoded for that field.
     *
     * The field path refers to the field and its parent fields if it is nested.
     */
    dLevels: number[]

    valueCount: number

    size: number
}

export interface ParquetDataPageData extends ShrededColumn {
    rowCount: number
}

export type ParquetColumnChunkData = ParquetDataPageData[]

export interface RowGroupData {
    rowCount: number
    columnData: Record<string, ParquetColumnChunkData>
    size: number
}
