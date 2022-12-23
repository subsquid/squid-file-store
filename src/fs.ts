import {
    GetObjectCommand,
    PutObjectCommand,
    S3Client,
    ListObjectsCommand,
    DeleteObjectsCommand,
} from '@aws-sdk/client-s3'
import assert from 'assert'
import fs from 'fs/promises'
import {existsSync} from 'fs'
import path from 'path'

export abstract class FS {
    async init(): Promise<void> {
        throw new Error('Method not implemented.')
    }

    async exist(name: string): Promise<boolean> {
        throw new Error('Method not implemented.')
    }

    async readFile(name: string, encoding: BufferEncoding): Promise<string> {
        throw new Error('Method not implemented.')
    }

    async writeFile(name: string, data: string, encoding: BufferEncoding): Promise<void> {
        throw new Error('Method not implemented.')
    }

    async transact(dir: string, f: (fs: FS) => Promise<void>): Promise<void> {
        throw new Error('Method not implemented.')
    }

    async mkdir(dir: string) {
        throw new Error('Method not implemented.')
    }

    async remove(dir: string) {
        throw new Error('Method not implemented.')
    }

    abs(name: string) {
        throw new Error('Method not implemented.')
    }
}

export class LocalFS extends FS {
    constructor(protected dir: string) {
        super()
        dir = path.resolve(dir)
    }

    async init(): Promise<void> {
        await fs.mkdir(this.dir, {recursive: true})
    }

    async exist(name: string) {
        return existsSync(path.join(this.dir, name))
    }

    async readFile(name: string, encoding: BufferEncoding): Promise<string> {
        await this.mkdir(path.dirname(name))
        return fs.readFile(this.abs(name), encoding)
    }

    async writeFile(name: string, data: string, encoding: BufferEncoding): Promise<void> {
        await this.mkdir(path.dirname(name))
        return fs.writeFile(this.abs(name), data, encoding)
    }

    async transact(dir: string, f: (fs: LocalFS) => Promise<void>) {
        let absPath = this.abs(this.dir, dir)
        let tempName = `${path.basename(absPath)}-temp-${Date.now()}`
        let tempDir = this.abs(path.dirname(absPath), tempName)
        let txFs = new LocalFS(tempDir)
        try {
            await f(txFs)
            await this.remove(dir)
            await fs.rename(tempDir, absPath)
        } catch (e) {
            await fs.rm(tempDir, {recursive: true, force: true})
            throw e
        }
    }

    async mkdir(dir: string) {
        await fs.mkdir(this.abs(this.dir, dir), {recursive: true})
    }

    async remove(name: string): Promise<void> {
        if (!(await this.exist(name))) return
        await fs.rm(this.abs(this.dir, name), {recursive: true, force: true})
    }

    abs(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }
}

export class S3Fs extends FS {
    constructor(private dir: string, private client: S3Client, private bucket: string) {
        super()
    }

    async init(): Promise<void> {}

    async exist(name: string) {
        let res = await this.client.send(
            new ListObjectsCommand({
                Bucket: this.bucket,
                Prefix: this.pathJoin(this.dir, name),
                MaxKeys: 1,
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

    private pathJoin(...paths: string[]) {
        return paths.reduce((res, path, i) => res + (path.startsWith('/') || i == 0 ? path : '/' + path), '')
    }

    async remove(name: string): Promise<void> {
        let ls = await this.client.send(
            new ListObjectsCommand({
                Bucket: this.bucket,
                Prefix: this.pathJoin(this.dir, name),
            })
        )
        let objects = ls.Contents?.map(({Key}) => ({Key}))
        if (!objects || objects.length == 0) return

        await this.client.send(
            new DeleteObjectsCommand({
                Bucket: this.bucket,
                Delete: {
                    Objects: objects as any,
                    Quiet: true,
                },
            })
        )
        if (ls.IsTruncated) await this.remove(name)
    }

    abs(...paths: string[]) {
        return this.pathJoin(this.dir, ...paths)
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
        throw new Error(`Unexpected destination - '${dest}'`)
    }
}

function parseUrl(url: string) {
    try {
        return new URL(url)
    } catch {
        return undefined
    }
}
