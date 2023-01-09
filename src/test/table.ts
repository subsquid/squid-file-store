import {Table, types, List, TableRecord} from '../table'

export let table = new Table('test', {
    string: types.string,
    int: types.number,
    float: types.number,
    bigint: types.bigint,
    boolean: types.boolean,
    timestamp: types.timestamp,
    nullableString: {type: types.string, nullable: true},
    list: List(types.string),
    nullableList: List(types.string, {nullable: true}),
})

type Record = TableRecord<typeof table>

export let record1: Record = {
    string: 'string',
    int: 3,
    bigint: 4n,
    float: 0.1,
    boolean: true,
    timestamp: new Date(),
    nullableString: null,
    list: ['a', 'b', 'c'],
    nullableList: ['a', null, 'c'],
}

export let record2: Record = {
    string: 'string',
    int: 34684631,
    bigint: 448468676564n,
    float: 0.1111111111111,
    boolean: true,
    timestamp: new Date(),
    nullableString: null,
    list: ['{}', ',', '"'],
    nullableList: [null, null, null],
}
