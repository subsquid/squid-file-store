import {Type} from './utils'

export class ListType<T> extends Type<(T | null | undefined)[], string> {
    readonly isList = true

    constructor(itemType: Type<T, any>) {
        super({
            dbType: `${itemType.dbType}[]`,
            serialize(value: (T | null | undefined)[]) {
                return `[${value.map((i) => (i == null ? null : itemType.serialize(i))).join(`, `)}]`
            },
        })
    }
}

export let List = <T>(itemType: Type<T, any>) => {
    return new ListType(itemType)
}
