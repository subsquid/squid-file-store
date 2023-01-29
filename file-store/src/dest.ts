import {existsSync} from 'fs'
import fs from 'fs/promises'
import path from 'upath'

export interface Dest {
    readFile(file: string): Promise<string>
    writeFile(file: string, data: string | Uint8Array): Promise<void>
    exists(path: string): Promise<boolean>
    mkdir(path: string): Promise<void> // always recursive
    readdir(path: string): Promise<string[]>
    rm(path: string): Promise<void> // always recursive and force
    transact(path: string, cb: (txDest: Dest) => Promise<void>): Promise<void>
    path(...paths: string[]): string
}

export class LocalDest implements Dest {
    protected dir: string

    constructor(dir: string) {
        this.dir = path.normalize(dir)
    }

    async exists(name: string) {
        return existsSync(this.path(name))
    }

    async readFile(name: string): Promise<string> {
        return fs.readFile(this.path(name), 'utf-8')
    }

    async writeFile(name: string, data: string | Uint8Array): Promise<void> {
        let destPath = this.path(name)
        await this.mkdir(path.dirname(destPath))
        return fs.writeFile(destPath, data, 'utf-8')
    }

    async rm(name: string): Promise<void> {
        return fs.rm(this.path(name), {recursive: true, force: true})
    }

    async readdir(dir: string): Promise<string[]> {
        if (await this.exists(dir)) {
            return fs.readdir(this.path(dir))
        } else {
            return []
        }
    }

    async mkdir(dir: string): Promise<void> {
        await fs.mkdir(dir, {recursive: true})
    }

    async rename(oldPath: string, newPath: string): Promise<void> {
        return fs.rename(this.path(oldPath), this.path(newPath))
    }

    async transact(dir: string, cb: (txDest: LocalDest) => Promise<void>) {
        let destPath = this.path(dir)
        let tempDir = `${path.basename(destPath)}-temp-${Date.now()}`
        let tempPath = path.join(path.dirname(destPath), tempDir)
        let txFs = new LocalDest(tempPath)
        try {
            await cb(txFs)
            await this.rm(dir)
            await this.rename(tempDir, dir)
        } catch (e) {
            await this.rm(tempDir)
            throw e
        }
    }

    path(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }
}

export function createDest(dest: string, s3Options?: any): Dest {
    let url = parseUrl(dest)
    if (!url) {
        return new LocalDest(dest)
    } else if (url.protocol === 's3:') {
        throw new Error('Package `@subsquid/file-s3` is not installed')
    } else {
        throw new Error(`Unexpected destination: "${dest}"`)
    }
}

function parseUrl(url: string) {
    try {
        return new URL(url)
    } catch {
        return undefined
    }
}
