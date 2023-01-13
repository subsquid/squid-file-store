export type Type<T> = {
    validate(value: unknown): T
}

export interface FieldData<T extends Type<any>> {
    type: T
    nullable?: boolean
}

export type Field<T extends Type<any> = Type<any>> = T | FieldData<T>

type NullableFields<T extends Record<string, Field>> = {
    [F in keyof T]: T[F] extends {nullable: true} ? F : never
}[keyof T]

export type ConvertFieldsToTypes<T extends Record<string, Field>> = {
    [F in Exclude<keyof T, NullableFields<T>>]: T[F] extends Field<Type<infer R>> ? R : never
} & {
    [F in Extract<keyof T, NullableFields<T>>]?: T[F] extends Field<Type<infer R>> ? R | null | undefined : never
}

export function getFieldData<T extends Type<any>>(field: Field<T>): FieldData<T> {
    return 'type' in field ? field : {type: field}
}

export interface TableHeader<> {
    [field: string]: Field<Type<any>>
}
export abstract class Table<T extends TableHeader> {
    readonly fields: Readonly<{
        name: string
        data: any
    }>[] = []

    constructor(readonly name: string, protected header: T) {
        for (let field in header) {
            this.fields.push({
                name: field,
                data: getFieldData(header[field]),
            })
        }
    }

    abstract createTableBuilder(): TableBuilder<T>
}

export type TableRecord<T extends TableHeader | Table<any>> = T extends Table<infer R>
    ? ConvertFieldsToTypes<R>
    : T extends TableHeader
    ? ConvertFieldsToTypes<T>
    : never

export interface TableBuilder<T extends TableHeader> {
    get size(): number

    append(records: TableRecord<T> | TableRecord<T>[]): TableBuilder<T>
    toTable(options: any): string
}

export interface TableBuilderContructor {
    new <T extends TableHeader>(table: Table<TableHeader>): TableBuilder<T>
}

// let a = new Table('aaa', {
//     a: {} as Type<string>
// })

// type A = TableRecord<typeof a>
