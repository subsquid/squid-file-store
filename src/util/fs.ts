import {S3Client, ListObjectsCommand, DeleteObjectsCommand} from '@aws-sdk/client-s3'
import assert from 'assert'
import fs from 'fs/promises'
import {existsSync} from 'fs'
import path from 'upath'

export abstract class FS {
    async exist(name: string): Promise<boolean> {
        throw new Error('Method not implemented.')
    }

    async ensure(dir: string) {
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

    async ensure(dir: string) {
        await this.init()
        await fs.mkdir(this.abs(dir), {recursive: true})
    }

    async remove(name: string): Promise<void> {
        await this.init()
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
