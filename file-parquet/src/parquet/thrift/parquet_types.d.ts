//
// Autogenerated by Thrift Compiler (0.18.0)
//
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
//
import Int64 = require('node-int64')

/**
 * Types supported by Parquet.  These types are intended to be used in combination
 * with the encodings to control the on disk storage format.
 * For example INT16 is not included as a type since a good encoding of INT32
 * would handle this.
 */
export declare enum Type {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
}

/**
 * DEPRECATED: Common types used by frameworks(e.g. hive, pig) using parquet.
 * ConvertedType is superseded by LogicalType.  This enum should not be extended.
 *
 * See LogicalTypes.md for conversion between ConvertedType and LogicalType.
 */
export declare enum ConvertedType {
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21,
}

/**
 * Representation of Schemas
 */
export declare enum FieldRepetitionType {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
}

/**
 * Encodings supported by Parquet.  Not all encodings are valid for all types.  These
 * enums are also used to specify the encoding of definition and repetition levels.
 * See the accompanying doc for the details of the more complicated encodings.
 */
export declare enum Encoding {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8,
    BYTE_STREAM_SPLIT = 9,
}

/**
 * Supported compression algorithms.
 *
 * Codecs added in format version X.Y can be read by readers based on X.Y and later.
 * Codec support may vary between readers based on the format version and
 * libraries available at runtime.
 *
 * See Compression.md for a detailed specification of these algorithms.
 */
export declare enum CompressionCodec {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6,
    LZ4_RAW = 7,
}

export declare enum PageType {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3,
}

/**
 * Enum to annotate whether lists of min/max elements inside ColumnIndex
 * are ordered and if so, in which direction.
 */
export declare enum BoundaryOrder {
    UNORDERED = 0,
    ASCENDING = 1,
    DESCENDING = 2,
}

/**
 * Statistics per row group and per page
 * All fields are optional.
 */
export declare class Statistics {
    max?: Buffer
    min?: Buffer
    null_count?: Int64
    distinct_count?: Int64
    max_value?: Buffer
    min_value?: Buffer

    constructor(args?: {
        max?: Buffer
        min?: Buffer
        null_count?: Int64
        distinct_count?: Int64
        max_value?: Buffer
        min_value?: Buffer
    })
}

/**
 * Empty structs to use as logical type annotations
 */
export declare class StringType {}

export declare class UUIDType {}

export declare class MapType {}

export declare class ListType {}

export declare class EnumType {}

export declare class DateType {}

/**
 * Logical type to annotate a column that is always null.
 *
 * Sometimes when discovering the schema of existing data, values are always
 * null and the physical type can't be determined. This annotation signals
 * the case where the physical type was guessed from all null values.
 */
export declare class NullType {}

/**
 * Decimal logical type annotation
 *
 * To maintain forward-compatibility in v1, implementations using this logical
 * type must also set scale and precision on the annotated SchemaElement.
 *
 * Allowed for physical types: INT32, INT64, FIXED, and BINARY
 */
export declare class DecimalType {
    scale: number
    precision: number

    constructor(args?: {scale: number; precision: number})
}

/**
 * Time units for logical types
 */
export declare class MilliSeconds {}

export declare class MicroSeconds {}

export declare class NanoSeconds {}

export declare class TimeUnit {
    MILLIS?: MilliSeconds
    MICROS?: MicroSeconds
    NANOS?: NanoSeconds

    constructor(args?: {MILLIS?: MilliSeconds; MICROS?: MicroSeconds; NANOS?: NanoSeconds})
}

/**
 * Timestamp logical type annotation
 *
 * Allowed for physical types: INT64
 */
export declare class TimestampType {
    isAdjustedToUTC: boolean
    unit: TimeUnit

    constructor(args?: {isAdjustedToUTC: boolean; unit: TimeUnit})
}

/**
 * Time logical type annotation
 *
 * Allowed for physical types: INT32 (millis), INT64 (micros, nanos)
 */
export declare class TimeType {
    isAdjustedToUTC: boolean
    unit: TimeUnit

    constructor(args?: {isAdjustedToUTC: boolean; unit: TimeUnit})
}

/**
 * Integer logical type annotation
 *
 * bitWidth must be 8, 16, 32, or 64.
 *
 * Allowed for physical types: INT32, INT64
 */
export declare class IntType {
    bitWidth: any
    isSigned: boolean

    constructor(args?: {bitWidth: any; isSigned: boolean})
}

/**
 * Embedded JSON logical type annotation
 *
 * Allowed for physical types: BINARY
 */
export declare class JsonType {}

/**
 * Embedded BSON logical type annotation
 *
 * Allowed for physical types: BINARY
 */
export declare class BsonType {}

/**
 * LogicalType annotations to replace ConvertedType.
 *
 * To maintain compatibility, implementations using LogicalType for a
 * SchemaElement must also set the corresponding ConvertedType (if any)
 * from the following table.
 */
export declare class LogicalType {
    STRING?: StringType
    MAP?: MapType
    LIST?: ListType
    ENUM?: EnumType
    DECIMAL?: DecimalType
    DATE?: DateType
    TIME?: TimeType
    TIMESTAMP?: TimestampType
    INTEGER?: IntType
    UNKNOWN?: NullType
    JSON?: JsonType
    BSON?: BsonType
    UUID?: UUIDType

    constructor(args?: {
        STRING?: StringType
        MAP?: MapType
        LIST?: ListType
        ENUM?: EnumType
        DECIMAL?: DecimalType
        DATE?: DateType
        TIME?: TimeType
        TIMESTAMP?: TimestampType
        INTEGER?: IntType
        UNKNOWN?: NullType
        JSON?: JsonType
        BSON?: BsonType
        UUID?: UUIDType
    })
}

/**
 * Represents a element inside a schema definition.
 *  - if it is a group (inner node) then type is undefined and num_children is defined
 *  - if it is a primitive type (leaf) then type is defined and num_children is undefined
 * the nodes are listed in depth first traversal order.
 */
export declare class SchemaElement {
    type?: Type
    type_length?: number
    repetition_type?: FieldRepetitionType
    name: string
    num_children?: number
    converted_type?: ConvertedType
    scale?: number
    precision?: number
    field_id?: number
    logicalType?: LogicalType

    constructor(args?: {
        type?: Type
        type_length?: number
        repetition_type?: FieldRepetitionType
        name: string
        num_children?: number
        converted_type?: ConvertedType
        scale?: number
        precision?: number
        field_id?: number
        logicalType?: LogicalType
    })
}

/**
 * Data page header
 */
export declare class DataPageHeader {
    num_values: number
    encoding: Encoding
    definition_level_encoding: Encoding
    repetition_level_encoding: Encoding
    statistics?: Statistics

    constructor(args?: {
        num_values: number
        encoding: Encoding
        definition_level_encoding: Encoding
        repetition_level_encoding: Encoding
        statistics?: Statistics
    })
}

export declare class IndexPageHeader {}

/**
 * The dictionary page must be placed at the first position of the column chunk
 * if it is partly or completely dictionary encoded. At most one dictionary page
 * can be placed in a column chunk.
 *
 */
export declare class DictionaryPageHeader {
    num_values: number
    encoding: Encoding
    is_sorted?: boolean

    constructor(args?: {num_values: number; encoding: Encoding; is_sorted?: boolean})
}

/**
 * New page format allowing reading levels without decompressing the data
 * Repetition and definition levels are uncompressed
 * The remaining section containing the data is compressed if is_compressed is true
 *
 */
export declare class DataPageHeaderV2 {
    num_values: number
    num_nulls: number
    num_rows: number
    encoding: Encoding
    definition_levels_byte_length: number
    repetition_levels_byte_length: number
    is_compressed?: boolean
    statistics?: Statistics

    constructor(args?: {
        num_values: number
        num_nulls: number
        num_rows: number
        encoding: Encoding
        definition_levels_byte_length: number
        repetition_levels_byte_length: number
        is_compressed?: boolean
        statistics?: Statistics
    })
}

/**
 * Block-based algorithm type annotation. *
 */
export declare class SplitBlockAlgorithm {}

/**
 * The algorithm used in Bloom filter. *
 */
export declare class BloomFilterAlgorithm {
    BLOCK?: SplitBlockAlgorithm

    constructor(args?: {BLOCK?: SplitBlockAlgorithm})
}

/**
 * Hash strategy type annotation. xxHash is an extremely fast non-cryptographic hash
 * algorithm. It uses 64 bits version of xxHash.
 *
 */
export declare class XxHash {}

/**
 * The hash function used in Bloom filter. This function takes the hash of a column value
 * using plain encoding.
 *
 */
export declare class BloomFilterHash {
    XXHASH?: XxHash

    constructor(args?: {XXHASH?: XxHash})
}

/**
 * The compression used in the Bloom filter.
 *
 */
export declare class Uncompressed {}

export declare class BloomFilterCompression {
    UNCOMPRESSED?: Uncompressed

    constructor(args?: {UNCOMPRESSED?: Uncompressed})
}

/**
 * Bloom filter header is stored at beginning of Bloom filter data of each column
 * and followed by its bitset.
 *
 */
export declare class BloomFilterHeader {
    numBytes: number
    algorithm: BloomFilterAlgorithm
    hash: BloomFilterHash
    compression: BloomFilterCompression

    constructor(args?: {
        numBytes: number
        algorithm: BloomFilterAlgorithm
        hash: BloomFilterHash
        compression: BloomFilterCompression
    })
}

export declare class PageHeader {
    type: PageType
    uncompressed_page_size: number
    compressed_page_size: number
    crc?: number
    data_page_header?: DataPageHeader
    index_page_header?: IndexPageHeader
    dictionary_page_header?: DictionaryPageHeader
    data_page_header_v2?: DataPageHeaderV2

    constructor(args?: {
        type: PageType
        uncompressed_page_size: number
        compressed_page_size: number
        crc?: number
        data_page_header?: DataPageHeader
        index_page_header?: IndexPageHeader
        dictionary_page_header?: DictionaryPageHeader
        data_page_header_v2?: DataPageHeaderV2
    })
}

/**
 * Wrapper struct to store key values
 */
export declare class KeyValue {
    key: string
    value?: string

    constructor(args?: {key: string; value?: string})
}

/**
 * Wrapper struct to specify sort order
 */
export declare class SortingColumn {
    column_idx: number
    descending: boolean
    nulls_first: boolean

    constructor(args?: {column_idx: number; descending: boolean; nulls_first: boolean})
}

/**
 * statistics of a given page type and encoding
 */
export declare class PageEncodingStats {
    page_type: PageType
    encoding: Encoding
    count: number

    constructor(args?: {page_type: PageType; encoding: Encoding; count: number})
}

/**
 * Description for column metadata
 */
export declare class ColumnMetaData {
    type: Type
    encodings: Encoding[]
    path_in_schema: string[]
    codec: CompressionCodec
    num_values: Int64
    total_uncompressed_size: Int64
    total_compressed_size: Int64
    key_value_metadata?: KeyValue[]
    data_page_offset: Int64
    index_page_offset?: Int64
    dictionary_page_offset?: Int64
    statistics?: Statistics
    encoding_stats?: PageEncodingStats[]
    bloom_filter_offset?: Int64

    constructor(args?: {
        type: Type
        encodings: Encoding[]
        path_in_schema: string[]
        codec: CompressionCodec
        num_values: Int64
        total_uncompressed_size: Int64
        total_compressed_size: Int64
        key_value_metadata?: KeyValue[]
        data_page_offset: Int64
        index_page_offset?: Int64
        dictionary_page_offset?: Int64
        statistics?: Statistics
        encoding_stats?: PageEncodingStats[]
        bloom_filter_offset?: Int64
    })
}

export declare class EncryptionWithFooterKey {}

export declare class EncryptionWithColumnKey {
    path_in_schema: string[]
    key_metadata?: Buffer

    constructor(args?: {path_in_schema: string[]; key_metadata?: Buffer})
}

export declare class ColumnCryptoMetaData {
    ENCRYPTION_WITH_FOOTER_KEY?: EncryptionWithFooterKey
    ENCRYPTION_WITH_COLUMN_KEY?: EncryptionWithColumnKey

    constructor(args?: {
        ENCRYPTION_WITH_FOOTER_KEY?: EncryptionWithFooterKey
        ENCRYPTION_WITH_COLUMN_KEY?: EncryptionWithColumnKey
    })
}

export declare class ColumnChunk {
    file_path?: string
    file_offset: Int64
    meta_data?: ColumnMetaData
    offset_index_offset?: Int64
    offset_index_length?: number
    column_index_offset?: Int64
    column_index_length?: number
    crypto_metadata?: ColumnCryptoMetaData
    encrypted_column_metadata?: Buffer

    constructor(args?: {
        file_path?: string
        file_offset: Int64
        meta_data?: ColumnMetaData
        offset_index_offset?: Int64
        offset_index_length?: number
        column_index_offset?: Int64
        column_index_length?: number
        crypto_metadata?: ColumnCryptoMetaData
        encrypted_column_metadata?: Buffer
    })
}

export declare class RowGroup {
    columns: ColumnChunk[]
    total_byte_size: Int64
    num_rows: Int64
    sorting_columns?: SortingColumn[]
    file_offset?: Int64
    total_compressed_size?: Int64
    ordinal?: number

    constructor(args?: {
        columns: ColumnChunk[]
        total_byte_size: Int64
        num_rows: Int64
        sorting_columns?: SortingColumn[]
        file_offset?: Int64
        total_compressed_size?: Int64
        ordinal?: number
    })
}

/**
 * Empty struct to signal the order defined by the physical or logical type
 */
export declare class TypeDefinedOrder {}

/**
 * Union to specify the order used for the min_value and max_value fields for a
 * column. This union takes the role of an enhanced enum that allows rich
 * elements (which will be needed for a collation-based ordering in the future).
 *
 * Possible values are:
 * * TypeDefinedOrder - the column uses the order defined by its logical or
 *                      physical type (if there is no logical type).
 *
 * If the reader does not support the value of this union, min and max stats
 * for this column should be ignored.
 */
export declare class ColumnOrder {
    TYPE_ORDER?: TypeDefinedOrder

    constructor(args?: {TYPE_ORDER?: TypeDefinedOrder})
}

export declare class PageLocation {
    offset: Int64
    compressed_page_size: number
    first_row_index: Int64

    constructor(args?: {offset: Int64; compressed_page_size: number; first_row_index: Int64})
}

export declare class OffsetIndex {
    page_locations: PageLocation[]

    constructor(args?: {page_locations: PageLocation[]})
}

/**
 * Description for ColumnIndex.
 * Each <array-field>[i] refers to the page at OffsetIndex.page_locations[i]
 */
export declare class ColumnIndex {
    null_pages: boolean[]
    min_values: Buffer[]
    max_values: Buffer[]
    boundary_order: BoundaryOrder
    null_counts?: Int64[]

    constructor(args?: {
        null_pages: boolean[]
        min_values: Buffer[]
        max_values: Buffer[]
        boundary_order: BoundaryOrder
        null_counts?: Int64[]
    })
}

export declare class AesGcmV1 {
    aad_prefix?: Buffer
    aad_file_unique?: Buffer
    supply_aad_prefix?: boolean

    constructor(args?: {aad_prefix?: Buffer; aad_file_unique?: Buffer; supply_aad_prefix?: boolean})
}

export declare class AesGcmCtrV1 {
    aad_prefix?: Buffer
    aad_file_unique?: Buffer
    supply_aad_prefix?: boolean

    constructor(args?: {aad_prefix?: Buffer; aad_file_unique?: Buffer; supply_aad_prefix?: boolean})
}

export declare class EncryptionAlgorithm {
    AES_GCM_V1?: AesGcmV1
    AES_GCM_CTR_V1?: AesGcmCtrV1

    constructor(args?: {AES_GCM_V1?: AesGcmV1; AES_GCM_CTR_V1?: AesGcmCtrV1})
}

/**
 * Description for file metadata
 */
export declare class FileMetaData {
    version: number
    schema: SchemaElement[]
    num_rows: Int64
    row_groups: RowGroup[]
    key_value_metadata?: KeyValue[]
    created_by?: string
    column_orders?: ColumnOrder[]
    encryption_algorithm?: EncryptionAlgorithm
    footer_signing_key_metadata?: Buffer

    constructor(args?: {
        version: number
        schema: SchemaElement[]
        num_rows: Int64
        row_groups: RowGroup[]
        key_value_metadata?: KeyValue[]
        created_by?: string
        column_orders?: ColumnOrder[]
        encryption_algorithm?: EncryptionAlgorithm
        footer_signing_key_metadata?: Buffer
    })
}

/**
 * Crypto metadata for files with encrypted footer *
 */
export declare class FileCryptoMetaData {
    encryption_algorithm: EncryptionAlgorithm
    key_metadata?: Buffer

    constructor(args?: {encryption_algorithm: EncryptionAlgorithm; key_metadata?: Buffer})
}
