/**
 * Supported approaches to quoting the serialized CSV values.
 */
export const enum Quote {
    /**
     * Put all values in quotes.
     */
    ALL,

    /**
     * Only quote strings with special characters.
     * A special character is one of the following:
     * delimiter, lineterminator, quoteChar.
     */
    MINIMAL,

    /**
     * Quote strings, booleans and DateTimes.
     */
    NONNUMERIC,

    /**
     * Do not quote values.
     */
    NONE,
}

/**
 * Interface for objects that detail how CSV-like output
 * files should be formatted.
 */
export interface Dialect {
    delimiter: string
    escapeChar?: string
    quoteChar: string
    quoting: Quote
    lineterminator: string
}

let excel: Dialect = {
    delimiter: ',',
    quoteChar: '"',
    quoting: Quote.MINIMAL,
    lineterminator: '\r\n'
}

let excelTab: Dialect = {
    ...excel,
    delimiter: '\t',
}

/**
 * Dialect presets.
 */
export let dialects = {
    /**
     * Use the formatting conventions of MS Excel CSV exports.
     */
    excel,

    /**
     * Use the formatting conventions of MS Excel TSV
     * (tab separated values) exports.
     */
    excelTab,
}
