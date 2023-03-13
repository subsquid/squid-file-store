export interface ShrededColumn {
    values: any[]
    rLevels: number[]
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
