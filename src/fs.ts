import {GetObjectCommand, PutObjectCommand, S3Client, ListObjectsCommand} from '@aws-sdk/client-s3'
import assert from 'assert'
import fs from 'fs/promises'
import {existsSync} from 'fs'
import path from 'path'

export abstract class FS {
    async exist(name: string): Promise<boolean> {
        throw new Error('Method not implemented.')
    }

    async readFile(path: string, encoding: BufferEncoding): Promise<string> {
        throw new Error('Method not implemented.')
    }

    async writeFile(path: string, data: string, encoding: BufferEncoding): Promise<void> {
        throw new Error('Method not implemented.')
    }

    async transact(dir: string, f: (fs: FS) => Promise<void>): Promise<void> {
        throw new Error('Method not implemented.')
    }
}

export class LocalFS extends FS {
    constructor(protected dir: string) {
        super()
    }

    async exist(name: string) {
        return existsSync(path.join(this.dir, name))
    }

    async readFile(name: string, encoding: BufferEncoding): Promise<string> {
        return fs.readFile(path.join(this.dir, name), encoding)
    }

    async writeFile(name: string, data: string, encoding: BufferEncoding): Promise<void> {
        let absPath = path.join(this.dir, name)
        await fs.mkdir(path.dirname(absPath), {recursive: true})
        return fs.writeFile(absPath, data, encoding)
    }

    async transact(dir: string, f: (fs: LocalFS) => Promise<void>) {
        let absPath = path.join(this.dir, dir)
        let tempName = `${path.basename(absPath)}-temp-${Date.now()}`
        let tempDir = path.join(path.dirname(absPath), tempName)
        let txFs = new LocalFS(tempDir)
        try {
            await f(txFs)
            await fs.rename(tempDir, absPath)
        } catch (e) {
            await fs.rm(tempDir, {recursive: true, force: true})
            throw e
        }
    }
}

export class S3Fs extends FS {
    constructor(private dir: string, private client: S3Client, private bucket: string) {
        super()
    }

    async exist(name: string) {
        let res = await this.client.send(
            new ListObjectsCommand({
                Bucket: this.bucket,
                Prefix: this.pathJoin(this.dir, name),
            })
        )
        return res.Contents?.length ? true : false
    }

    async readFile(name: string, encoding: BufferEncoding): Promise<string> {
        let res = await this.client.send(
            new GetObjectCommand({
                Bucket: this.bucket,
                Key: this.pathJoin(this.dir, name),
            })
        )
        if (res.Body) {
            return res.Body.transformToString(encoding)
        } else {
            throw new Error()
        }
    }

    async writeFile(name: string, data: string, encoding: BufferEncoding): Promise<void> {
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.pathJoin(this.dir, name),
                Body: Buffer.from(data, encoding),
            })
        )
    }

    async transact(dir: string, f: (fs: S3Fs) => Promise<void>): Promise<void> {
        let txFs = new S3Fs(this.pathJoin(this.dir, dir), this.client, this.bucket)
        await f(txFs) 
    }

    private pathJoin(...paths: string[]) {
        return paths.reduce((res, path, i) => res + (path.startsWith('/') || i == 0 ? path : '/' + path), '')
    }
}

export interface S3Options {
    endpoint: string
    region: string
    accessKey: string
    secretKey: string
}

export type FSOptions = S3Options

export function createFS(dest: string, fsOptions?: FSOptions) {
    let url = parseUrl(dest)
    if (!url) {
        return new LocalFS(dest)
    } else if (url.protocol === 's3:') {
        assert(fsOptions, 'Missing options for s3 destination')
        let s3client = new S3Client({
            region: fsOptions.region,
            endpoint: fsOptions.endpoint,
            credentials: {
                secretAccessKey: fsOptions.secretKey,
                accessKeyId: fsOptions.accessKey,
            },
        })
        return new S3Fs(url.pathname.slice(1), s3client, url.hostname)
    } else {
        throw new Error(`Unexpected destination - ${dest}`)
    }
}

function parseUrl(url: string) {
    try {
        return new URL(url)
    } catch {
        return undefined
    }
}
