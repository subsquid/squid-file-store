import {Table, TableRecord} from '../table'
import {List, Struct, types} from '../types'

let table = new Table('table', {
    a: types.string,
    b: types.int,
    c: types.float,
    d: types.boolean,
    e: types.bigint,
    f: {type: types.string, nullable: true},
    g: List(types.string),
    h: Struct({
        a: types.string,
        b: {type: types.boolean, nullable: true},
    }),
} as const)

type Record = TableRecord<typeof table>

let record: Record = {
    a: 'a',
    b: 1,
    c: 0.1,
    d: true,
    e: 1n,
    f: 'f',
    g: ['g'],
    h: {a: 'a', b: false},
}
