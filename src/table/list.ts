import assert from 'assert'
import {Type} from './utils'

interface ListOptions {
    nullable?: boolean
}

export let List = <T, O extends ListOptions>(
    itemType: Type<T>,
    options?: O
): Type<(O['nullable'] extends true ? T | null | undefined : T)[]> => ({
    dbType: `${itemType.dbType}[]`,
    serialize(value: (T | null | undefined)[]) {
        let items: string[] = []
        for (let i of value) {
            assert(i != null || options?.nullable, `NULL value in not nullable array`)
            let serializedItem = i == null ? 'NULL' : itemType.serialize(i)
            items.push(serializedItem.match(/[,]/) ? `'${serializedItem}'` : serializedItem)
        }
        return `[${items.join(`, `)}]`
    },
})
