import {Type, TypeField, TypeOptions} from './types'

export function normalizeTypeField(typeField: TypeField): TypeOptions {
    return typeField instanceof Type ? {type: typeField} : typeField
}
