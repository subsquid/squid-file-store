import {Type} from './types'

class ListType<T> extends Type<T[], string> {
    readonly isList = true

    constructor(itemType: Type<T, any>) {
        super({
            dbType: `${itemType.dbType}[]`,
            serialize(value: T[]) {
                return `[${value.map((i) => (i == null ? null : itemType.serialize(i))).join(`, `)}]`
            },
        })
    }
}

export let List = <T>(itemType: Type<T, any>): Type<T[], string> => {
    return new ListType(itemType)
}
