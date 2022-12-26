import {Type} from './utils'

export class ListType<T, N extends boolean = true> extends Type<(N extends true ? T | null | undefined : T)[], string> {
    readonly isList = true

    constructor(itemType: Type<T, any>, options?: {nullable?: N}) {
        super({
            dbType: `${itemType.dbType}[]`,
            serialize(value: (T | null | undefined)[]) {
                return `[${value.map((i) => (i == null ? 'NULL' : itemType.serialize(i))).join(`, `)}]`
            },
        })
    }
}

export let List = <T, N extends boolean = true>(itemType: Type<T, any>, options?: {nullable?: N}) => {
    return new ListType(itemType, options)
}
