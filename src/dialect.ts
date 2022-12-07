export const enum Quote {
    ALL,
    MINIMAL,
    NONNUMERIC,
    NONE,
}

export interface Dialect {
    delimiter: string
    arrayDelimiter: string
    escapeChar?: string
    lineTerminator: string
    quoteChar: string
    quoting: Quote
    header: boolean
}

let excel: Dialect = {
    delimiter: ',',
    arrayDelimiter: '|',
    quoteChar: '"',
    lineTerminator: '\r\n',
    quoting: Quote.MINIMAL,
    header: true,
}

export let dialects = {
    excel,
}
