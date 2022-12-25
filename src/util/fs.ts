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
import path from 'upath'

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

    async mkdir(dir: string) {
        throw new Error('Method not implemented.')
    }

    async remove(dir: string) {
        throw new Error('Method not implemented.')
    }

    abs(...paths: string[]) {
        throw new Error('Method not implemented.')
    }
}

export class LocalFS extends FS {
    constructor(protected dir: string) {
        super()
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

    async mkdir(dir: string) {
        await fs.mkdir(this.abs(dir), {recursive: true})
    }

    async remove(name: string): Promise<void> {
        let p = this.abs(this.dir, name)
        await fs.rm(this.abs(name), {recursive: true, force: true})
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

    async init(): Promise<void> {}

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

    async readFile(name: string, encoding: BufferEncoding): Promise<string> {
        let res = await this.client.send(
            new GetObjectCommand({
                Bucket: this.bucket,
                Key: this.relative(name),
            })
        )
        if (res.Body) {
            return res.Body.transformToString(encoding)
        } else {
            throw new Error()
        }
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

    async mkdir(dir: string) {}

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
    accessKeyId: string
    secretAccessKey: string
    sessionToken?: string
}

export function createFS(dest: string, fsOptions?: S3Options) {
    let url = parseUrl(dest)
    if (!url) {
        return new LocalFS(dest)
    } else if (url.protocol === 's3:') {
        assert(fsOptions, 'Missing options for s3 destination')
        let s3client = new S3Client({
            region: fsOptions.region,
            endpoint: fsOptions.endpoint,
            credentials: {
                secretAccessKey: fsOptions.secretAccessKey,
                accessKeyId: fsOptions.accessKeyId,
                sessionToken: fsOptions.sessionToken,
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
