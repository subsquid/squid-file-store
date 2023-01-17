import path from 'upath'
import fs from 'fs/promises'
import {existsSync} from 'fs'
import type {S3Options} from '@subsquid/bigdata-s3'

export interface FS {
    readFile(file: string): Promise<string>
    writeFile(file: string, data: string | Uint8Array): Promise<void>
    exists(path: string): Promise<boolean>
    mkdir(path: string): Promise<void> // always recursive
    readdir(path: string): Promise<string[]>
    rm(path: string): Promise<void> // always recursive and force
    abs(...paths: string[]): string
}

export class LocalFS implements FS {
    constructor(protected dir: string) {}

    async exists(name: string) {
        return existsSync(path.join(this.dir, name))
    }

    async readFile(name: string): Promise<string> {
        return fs.readFile(this.abs(name), 'utf-8')
    }

    async writeFile(name: string, data: string): Promise<void> {
        let absPath = this.abs(name)
        await this.mkdir(path.dirname(absPath))
        return fs.writeFile(absPath, data, 'utf-8')
    }

    async rm(name: string): Promise<void> {
        return fs.rm(this.abs(name), {recursive: true, force: true})
    }

    async readdir(dir: string): Promise<string[]> {
        return fs.readdir(dir)
    }

    async mkdir(dir: string): Promise<void> {
        await fs.mkdir(dir, {recursive: true})
    }

    async rename(oldPath: string, newPath: string): Promise<void> {
        return fs.rename(this.abs(oldPath), this.abs(newPath))
    }

    abs(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }
}

export async function fsTransact(fs: FS, dir: string, cb: (fs: FS) => Promise<void>) {
    if (fs instanceof LocalFS) {
        let absPath = fs.abs(dir)
        let tempDir = `${path.basename(absPath)}-temp-${Date.now()}`
        let tempPath = path.join(path.dirname(absPath), tempDir)
        let txFs = new LocalFS(tempPath)
        try {
            await cb(txFs)
            await fs.rm(dir)
            await fs.rename(tempDir, dir)
        } catch (e) {
            await fs.rm(tempDir)
            throw e
        }
    } else {
        await cb(fs)
    }
}

export function createFS(dest: string, s3Options?: S3Options) {
    let url = parseUrl(dest)
    if (!url) {
        return new LocalFS(dest)
    } else if (url.protocol === 's3:') {
        try {
            let s3 = require('@subsquid/bigdata-s3')
            return new s3.S3Fs(url.pathname.slice(1), url.hostname, s3Options)
        } catch (e) {
            throw new Error('Package `@subsquid/bigdata-s3` is not installed')
        }
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

export type {S3Options} from '@subsquid/bigdata-s3'
