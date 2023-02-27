// import * as Arrow from 'apache-arrow'
// import {Type} from '../table'

import assert from 'assert'
import {ConvertedType, LogicalType, ListType, FieldRepetitionType} from '../../thrift/parquet_types'
import {Type} from '../table'

interface ListOptions {
    nullable?: boolean
}

export let List = <T, Options extends ListOptions>(
    itemType: Type<T>,
    options?: Options
): Type<(Options['nullable'] extends true ? T | null | undefined : T)[]> => ({
    isNested: true,
    convertedType: ConvertedType.LIST,
    logicalType: new LogicalType({LIST: new ListType()}),
    children: {
        list: {
            type: {
                isNested: true as const,
                children: {
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
