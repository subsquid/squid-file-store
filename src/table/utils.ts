export class Type<T, R = T> {
    readonly dbType: string
    readonly serialize: (value: T) => R

    constructor(options: {dbType: string; serialize: (value: T) => R}) {
        this.dbType = options.dbType
        this.serialize = options.serialize
    }
}

export interface FieldData<T extends Type<any, any> = Type<any, any>> {
    type: T
    nullable?: boolean
}

export type Field<T extends Type<any, any> = Type<any, any>> = T | FieldData<T>

type NullableFields<T extends Record<string, Field>> = {
    [k in keyof T]: T[k] extends {nullable: true} ? k : never
}[keyof T]

export type ConvertFieldsToTypes<T extends Record<string, Field>> = {
    [k in keyof Omit<T, NullableFields<T>>]: T[k] extends Field<Type<infer R, any>> ? R : never
} & {
    [k in keyof Pick<T, NullableFields<T>>]?: T[k] extends Field<Type<infer R, any>> ? R | null | undefined : never
}

export function getFieldData(field: Field): FieldData {
    return field instanceof Type ? {type: field} : field
}
