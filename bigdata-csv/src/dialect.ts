export const enum Quote {
    ALL,
    MINIMAL,
    NONNUMERIC,
    NONE,
}

export interface Dialect {
    delimiter: string
    escapeChar?: string
    quoteChar: string
    quoting: Quote
}

let excel: Dialect = {
    delimiter: ',',
    quoteChar: '"',
    quoting: Quote.MINIMAL,
}

let excelTab: Dialect = {
    ...excel,
    delimiter: '\t',
}

export let dialects = {
    excel,
    excelTab,
}
