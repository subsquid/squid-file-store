import assert from 'assert'
import * as parquet from '../../thrift/parquet_types'
import {Type} from '../table'

interface ListOptions {
    nullable?: boolean
}

export let List = <T, Options extends ListOptions>(
    itemType: Type<T>,
    options?: Options
): Type<(Options['nullable'] extends true ? T | null | undefined : T)[]> => ({
    isNested: true,
    convertedType: parquet.ConvertedType.LIST,
    logicalType: new parquet.LogicalType({LIST: new parquet.ListType()}),
    fields: {
        list: {
            type: {
                isNested: true as const,
                fields: {
                    element: {
                        type: itemType,
                        repetition: options?.nullable ? 'OPTIONAL' : 'REQUIRED',
                    },
                },
                transform(value: T | null | undefined) {
                    if (value == null) {
                        assert(options?.nullable)
                        return {
                            element: undefined,
                        }
                    } else {
                        return {
                            element: itemType.isNested ? value : itemType.toPrimitive(value),
                        }
                    }
                },
            },
            repetition: 'REPEATED',
        },
    },
    transform(value: (T | null | undefined)[]) {
        return {list: value}
    },
})
