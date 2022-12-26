import assert from 'assert'
import {ConvertFieldsToTypes, Type, Field} from './utils'
import {getFieldData} from './utils'

export class StructType<T extends Record<string, Field>> extends Type<ConvertFieldsToTypes<T>, string> {
    readonly isStruct = true

    constructor(structType: T) {
        let fields = Object.entries(structType).map(([name, field]) => ({
            name,
            data: getFieldData(field),
        }))
        super({
            dbType: `STRUCT(${fields.map((field) => `"${field.name}" ${field.data.type.dbType}`)})`,
            serialize(struct: ConvertFieldsToTypes<T>) {
                let res = new Array(fields.length)
                for (let i = 0; i < fields.length; i++) {
                    let field = fields[i]
                    let value = struct[field.name as keyof typeof struct]
                    let serializedValue =
                        value == null ? null : field.data.type.serialize(struct[field.name as keyof typeof struct])
                    assert(!field.data.nullable || serializedValue != null)
                    res[i] = serializedValue
                }
                return `{${res.join(`, `)}}`
            },
        })
    }
}

export let Struct = <T extends Record<string, Field>>(structType: T) => {
    return new StructType(structType)
}
