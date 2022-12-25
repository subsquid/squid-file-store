import {Table, TableRecord} from '../table'
import {List, NotNull, Struct, types} from '../types'

let header = {
    a: types.string,
    b: types.int,
    c: types.float,
    d: types.boolean,
    e: types.bigint,
    f: NotNull(types.string),
    g: List(types.string),
    h: Struct({
        a: types.string,
    }),
}

let table = new Table('table', header)

type Record = TableRecord<typeof table>

let record: Record = {
    a: 'a',
    b: 1,
    c: 0.1,
    d: true,
    e: 1n,
    f: 'f',
    g: ['g'],
    h: {a: 'a'},
}
