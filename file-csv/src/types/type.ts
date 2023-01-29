export interface Type<T> {
    serialize(value: T): string
    validate(value: unknown): T
    isNumeric: boolean
}
