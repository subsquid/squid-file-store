export interface Type<T> {
    readonly dbType: string
    readonly serialize: (value: T) => string
}

export interface FieldData<T extends Type<any> = Type<any>> {
    type: T
    nullable?: boolean
}

export type Field<T extends Type<any> = Type<any>> = T | FieldData<T>

type NullableFields<T extends Record<string, Field>> = {
    [k in keyof T]: T[k] extends {nullable: true} ? k : never
}[keyof T]

export type ConvertFieldsToTypes<T extends Record<string, Field>> = {
    [k in keyof Omit<T, NullableFields<T>>]: T[k] extends Field<Type<infer R>> ? R : never
} & {
    [k in keyof Pick<T, NullableFields<T>>]?: T[k] extends Field<Type<infer R>> ? R | null | undefined : never
}

export function getFieldData(field: Field): FieldData {
    return 'type' in field ? field : {type: field}
}
