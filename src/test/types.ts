import {Table, TableRecord} from '../table'
import {Nullable, types, ArrayType} from '../types'

let header = {
    a: types.string,
    b: types.int,
    c: types.float,
    d: types.boolean,
    e: types.bigint,
    f: Nullable(types.string),
    g: ArrayType(types.string),
    h: ArrayType(Nullable(types.string)),
}

let table = new Table('table', header)

type Record = TableRecord<typeof table>

let record: Record = {
    a: 'a',
    b: 1,
    c: 0.1,
    d: true,
    e: 1n,
    g: ['g'],
    h: ['h', null, undefined],
}

record = {
    a: 'a',
    b: 1,
    c: 0.1,
    d: true,
    e: 1n,
    f: 'f',
    g: ['g'],
    h: ['h', null, undefined],
}
