import assert from 'assert'

export function createFolderName(from: number, to: number) {
    let name = from.toString().padStart(10, '0') + '-' + to.toString().padStart(10, '0')
    assert(isFolderName(name))
    return name
}

export function isFolderName(str: string) {
    return /^(\d+)-(\d+)$/.test(str)
}
