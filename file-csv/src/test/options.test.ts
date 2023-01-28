import expect from 'expect'
import {Quote, dialects} from '../dialect'
import {Column, Table} from '../table'
import {IntegerType, StringType} from '../types'

describe('Options', function () {
    describe('header', async function () {
        it('with header', () => {
            let table = new Table(
                'test',
                {column1: Column(StringType()), column2: Column(StringType())},
                {header: true}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'value1', column2: 'value2'})
            let result = builder.flush()

            expect(result).toEqual('column1,column2\r\n' + 'value1,value2\r\n')
        })

        it('without header', () => {
            let table = new Table(
                'test',
                {column1: Column(StringType()), column2: Column(StringType())},
                {header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'value1', column2: 'value2'})
            let result = builder.flush()

            expect(result).toEqual('value1,value2\r\n')
        })
    })

    describe('dialect', async function () {
        it('excel', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(StringType()),
                    column4: Column(StringType()),
                },
                {dialect: dialects.excel, header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb,b', column3: 'c"c"c', column4: 'dd\r\nd'})
            let result = builder.flush()

            expect(result).toEqual('aaa,"bb,b","c""c""c","dd\r\nd"\r\n')
        })

        it('excel-tab', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(StringType()),
                    column4: Column(StringType()),
                },
                {dialect: dialects.excelTab, header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb\tb', column3: 'c"c"c', column4: 'dd,d'})
            let result = builder.flush()

            expect(result).toEqual('aaa\t"bb\tb"\t"c""c""c"\tdd,d\r\n')
        })

        it('custom', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(StringType()),
                    column4: Column(StringType()),
                },
                {
                    dialect: {
                        delimiter: '-',
                        quoteChar: "'",
                        escapeChar: '^',
                        quoting: Quote.MINIMAL,
                        lineterminator: ';',
                    },
                    header: false,
                }
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb-b', column3: "c'c'c", column4: 'dd,d'})
            let result = builder.flush()

            expect(result).toEqual("aaa-'bb-b'-'c^'c^'c'-dd,d;")
        })

        it('Quote.NONE', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(IntegerType()),
                },
                {dialect: {...dialects.excel, quoting: Quote.NONE}, header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb,b', column3: 1000})
            let result = builder.flush()

            expect(result).toEqual('aaa,bb",b,1000\r\n')
        })

        it('Quote.MINIMAL', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(IntegerType()),
                },
                {dialect: {...dialects.excel, quoting: Quote.MINIMAL}, header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb,b', column3: 1000})
            let result = builder.flush()

            expect(result).toEqual('aaa,"bb,b",1000\r\n')
        })

        it('Quote.NONNUMERIC', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(IntegerType()),
                },
                {dialect: {...dialects.excel, quoting: Quote.NONNUMERIC}, header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb,b', column3: 1000})
            let result = builder.flush()

            expect(result).toEqual('"aaa","bb,b",1000\r\n')
        })

        it('Quote.ALL', () => {
            let table = new Table(
                'test',
                {
                    column1: Column(StringType()),
                    column2: Column(StringType()),
                    column3: Column(IntegerType()),
                },
                {dialect: {...dialects.excel, quoting: Quote.ALL}, header: false}
            )
            let builder = table.createTableBuilder()
            builder.append({column1: 'aaa', column2: 'bb,b', column3: 1000})
            let result = builder.flush()

            expect(result).toEqual('"aaa","bb,b","1000"\r\n')
        })
    })
})
