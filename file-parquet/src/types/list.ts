// import * as Arrow from 'apache-arrow'
// import {Type} from '../table'

// interface ListOptions {
//     nullable?: boolean
// }

// export let List = <T, Options extends ListOptions>(
//     itemType: Type<T>,
//     options?: Options
// ): Type<(Options['nullable'] extends true ? T | null | undefined : T)[]> => ({
//     arrowDataType: new Arrow.List(Arrow.Field.new('element', itemType.arrowDataType, options?.nullable)),
//     prepare(value) {
//         return value.map((i) => (i == null ? null : itemType.prepare(i)))
//     },
// })

// // export let MapType = <T>(itemType: Type<T>): Type<Map<string, T>> => ({
// //     arrowDataType: new Map_(
// //         Field.new(
// //             'key_value',
// //             new Struct([Field.new('key', new Utf8()), Field.new('value', itemType.arrowDataType) as any])
// //         )
// //     ),
// //     prepare(value) {
// //         return [...this.validate(value).entries()].map(([key, value]) => ({
// //             key,
// //             value,
// //         }))
// //     },
// //     validate(value) {
// //         assert(value instanceof Map)
// //         return value
// //     },
// // })
