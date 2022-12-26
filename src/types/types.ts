export class Type<T, R = T> {
    readonly dbType: string
    readonly serialize: (value: T) => R

    constructor(options: {dbType: string; serialize: (value: T) => R}) {
        this.dbType = options.dbType
        this.serialize = options.serialize
    }
}

export interface TypeOptions<T extends Type<any, any> = Type<any, any>> {
    type: T
    nullable?: boolean
}

export type TypeField<T extends Type<any, any> = Type<any, any>> = T | TypeOptions<T>

type NullableFields<T extends Record<string, TypeField>> = {
    [k in keyof T]: T[k] extends {nullable: true} ? k : never
}[keyof T]

export type ConvertFieldsToTypes<T extends Record<string, TypeField>> = {
    [k in keyof Omit<T, NullableFields<T>>]: T[k] extends TypeField<Type<infer R, any>> ? R : never
} & {
    [k in keyof Pick<T, NullableFields<T>>]?: T[k] extends TypeField<Type<infer R, any>> ? R | null | undefined : never
}
