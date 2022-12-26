import assert from 'assert'
import {Type, TypeField} from './types'
import {normalizeTypeField} from './utils'

class StructType<T extends Record<string, any>> extends Type<T, string> {
    readonly isStruct = true

    constructor(structType: {
        [k in keyof T]: TypeField<Type<T[k], any>>
    }) {
        let fields = Object.entries(structType).map(([name, fieldData]) => ({
            name,
            fieldData: normalizeTypeField(fieldData),
        }))
        super({
            dbType: `STRUCT(${fields.map(({name, fieldData}) => `"${name}" ${fieldData.type.dbType}`)})`,
            serialize(struct: T) {
                let res: string[] = []
                for (let {name, fieldData} of fields) {
                    let value = struct[name]
                    assert(!fieldData.nullable || value != null)
                    res.push(`'${name}': '${value == null ? null : fieldData.type.serialize(value)}'`)
                }
                return `{${res.join(`, `)}}`
            },
        })
    }
}

export let Struct = <T extends Record<string, any>>(structType: {
    [k in keyof T]: TypeField<Type<T[k], any>>
}): Type<T, string> => {
    return new StructType(structType)
}
