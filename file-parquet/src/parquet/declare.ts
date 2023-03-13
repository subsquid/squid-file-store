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
