import {
    S3Client,
    ListObjectsCommand,
    DeleteObjectsCommand,
    GetObjectCommand,
    PutObjectCommand,
} from '@aws-sdk/client-s3'
import fs from 'fs/promises'
import {existsSync} from 'fs'
import path from 'upath'
import {assertNotNull} from '@subsquid/util-internal'

export abstract class FS {
    async exist(name: string): Promise<boolean> {
        throw new Error('Method not implemented.')
    }

    async readFile(name: string): Promise<string> {
        throw new Error('Method not implemented.')
    }
    async writeFile(name: string, data: string): Promise<void> {
        throw new Error('Method not implemented.')
    }

    async ensure(dir: string) {
        throw new Error('Method not implemented.')
    }

    async remove(dir: string) {
        throw new Error('Method not implemented.')
    }

    async transact(dir: string, f: (fs: FS) => Promise<void>) {
        throw new Error('Method not implemented.')
    }

    abs(...paths: string[]) {
        throw new Error('Method not implemented.')
    }
}

export class LocalFS extends FS {
    private initialized = false

    constructor(protected dir: string) {
        super()
    }

    private async init(): Promise<LocalFS> {
        if (!this.initialized) {
            await fs.mkdir(this.dir, {recursive: true})
            this.initialized = true
        }
        return this
    }

    async exist(name: string) {
        await this.init()
        return existsSync(path.join(this.dir, name))
    }

    async readFile(name: string): Promise<string> {
        return fs.readFile(this.abs(name), 'utf-8')
    }

    async writeFile(name: string, data: string): Promise<void> {
        let absPath = this.abs(name)
        await fs.mkdir(path.dirname(absPath), {recursive: true})
        return fs.writeFile(absPath, data, 'utf-8')
    }

    async ensure(dir: string) {
        await this.init()
        await fs.mkdir(this.abs(dir), {recursive: true})
    }

    async remove(name: string): Promise<void> {
        await this.init()
        await fs.rm(this.abs(name), {recursive: true, force: true})
    }

    async transact(dir: string, f: (fs: LocalFS) => Promise<void>) {
        let absPath = path.join(this.dir, dir)
        let tempName = `${path.basename(absPath)}-temp-${Date.now()}`
        let tempDir = path.join(path.dirname(absPath), tempName)
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

    relative(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }

    abs(...paths: string[]) {
        return path.resolve(this.dir, ...paths)
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
                Prefix: this.relative(name),
                MaxKeys: 1,
            })
        )
        return res.Contents?.length ? true : false
    }

    async readFile(name: string): Promise<string> {
        let res = await this.client.send(
            new GetObjectCommand({
                Bucket: this.bucket,
                Key: this.relative(this.dir, name),
            })
        )
        if (res.Body) {
            return res.Body.transformToString('utf-8')
        } else {
            throw new Error()
        }
    }

    async writeFile(name: string, data: string): Promise<void> {
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.relative(this.dir, name),
                Body: Buffer.from(data, 'utf-8'),
            })
        )
    }

    async transact(dir: string, f: (fs: S3Fs) => Promise<void>): Promise<void> {
        let txFs = new S3Fs(this.relative(this.dir, dir), this.client, this.bucket)
        await f(txFs)
    }

    async remove(name: string): Promise<void> {
        let ls = await this.client.send(
            new ListObjectsCommand({
                Bucket: this.bucket,
                Prefix: this.relative(name),
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

    async ensure(dir: string) {}

    relative(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }

    abs(...paths: string[]) {
        return `s3://` + path.join(this.bucket, this.dir, ...paths)
    }
}

export interface S3Options {
    endpoint?: string
    region?: string
    accessKeyId?: string
    secretAccessKey?: string
}

export function createFS(dest: string, s3Options?: S3Options) {
    let url = parseUrl(dest)
    if (!url) {
        return new LocalFS(dest)
    } else if (url.protocol === 's3:') {
        let s3client = new S3Client(
            s3Options || {
                region: process.env.S3_REGION,
                endpoint: process.env.S3_ENDPOINT,
                credentials: {
                    secretAccessKey: assertNotNull(process.env.S3_SECRET_ACCESS_KEY),
                    accessKeyId: assertNotNull(process.env.S3_ACCESS_KEY_ID),
                },
            }
        )
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
