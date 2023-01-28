import {existsSync} from 'fs'
import fs from 'fs/promises'
import path from 'upath'
import {S3FsConstructor, S3Options} from '@subsquid/file-s3'

export interface FS {
    readFile(file: string): Promise<string>
    writeFile(file: string, data: string | Uint8Array): Promise<void>
    exists(path: string): Promise<boolean>
    mkdir(path: string): Promise<void> // always recursive
    readdir(path: string): Promise<string[]>
    rm(path: string): Promise<void> // always recursive and force
    path(...paths: string[]): string
}

export class LocalFS implements FS {
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

    path(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }
}

export async function fsTransact(fs: FS, dir: string, cb: (fs: FS) => Promise<void>) {
    if (fs instanceof LocalFS) {
        let destPath = fs.path(dir)
        let tempDir = `${path.basename(destPath)}-temp-${Date.now()}`
        let tempPath = path.join(path.dirname(destPath), tempDir)
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

export function createFS(dest: string, s3Options?: S3Options): FS {
    let url = parseUrl(dest)
    if (!url) {
        return new LocalFS(dest)
    } else if (url.protocol === 's3:') {
        try {
            let S3Fs = require('@subsquid/file-s3').S3Fs as S3FsConstructor
            return new S3Fs(url.pathname.slice(1), url.hostname, s3Options)
        } catch (e) {
            throw new Error('Package `@subsquid/file-s3` is not installed')
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

export type {S3Options} from '@subsquid/file-s3'
