// import assert from 'assert'
// import {ConvertFieldsToTypes, Type, Field} from './utils'
// import {getFieldData} from './utils'

// export class StructType<T extends Record<string, Field>> extends Type<ConvertFieldsToTypes<T>, string> {
//     readonly isStruct = true

//     constructor(structType: T) {
//         let fields = Object.entries(structType).map(([name, field]) => ({
//             name,
//             data: getFieldData(field),
//         }))
//         super({
//             dbType: `STRUCT(${fields.map((field) => `"${field.name}" ${field.data.type.dbType}`)})`,
//             serialize(struct: ConvertFieldsToTypes<T>) {
//                 let res = new Array(fields.length)
//                 for (let i = 0; i < fields.length; i++) {
//                     let field = fields[i]
//                     let value = struct[field.name as keyof typeof struct]
//                     assert(value != null || field.data.nullable, `NULL value in not nullable field '${field.name}'`)
//                     let serializedValue = value == null ? 'NULL' : field.data.type.serialize(value)
//                     res[i] = `"${field.name}": ${serializedValue}`
//                 }
//                 return `{${res.join(`, `)}}`
//             },
//         })
//     }
// }

// export let Struct = <T extends Record<string, Field>>(structType: T) => {
//     return new StructType(structType)
// }
