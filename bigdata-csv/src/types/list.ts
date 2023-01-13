import assert from 'assert'
import {CsvType} from './type'

interface ListOptions {
    nullable?: boolean
}

export let List = <T, Options extends ListOptions>(
    itemType: CsvType<T>,
    options?: Options
): CsvType<(Options['nullable'] extends true ? T | null | undefined : T)[]> => ({
    serialize(value) {
        let items: string[] = []
        for (let i of value) {
            assert(i != null || options?.nullable, `NULL value in not nullable array`)
            let serializedItem = i == null ? 'NULL' : itemType.serialize(i)
            items.push(serializedItem.match(/[,]/) ? `'${serializedItem}'` : serializedItem)
        }
        return `[${items.join(`, `)}]`
    },
    validate(value: unknown) {
        assert(Array.isArray(value))
        return value
    },
})
