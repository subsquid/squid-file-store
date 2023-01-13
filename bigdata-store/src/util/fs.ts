import {
    S3Client,
    DeleteObjectsCommand,
    GetObjectCommand,
    PutObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    NotFound,
} from '@aws-sdk/client-s3'
import fs from 'fs/promises'
import {existsSync} from 'fs'
import path from 'upath'
import {assertNotNull} from '@subsquid/util-internal'
import assert from 'assert'

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

export class S3Fs implements FS {
    constructor(private dir: string, private client: S3Client, private bucket: string) {}

    async exists(name: string) {
        if (name.endsWith('/')) {
            return this.existsDir(name)
        } else {
            let isFileExist = await this.existsFile(name)
            if (isFileExist) return true

            let isDirExist = await this.existsDir(name)
            if (isDirExist) return true
        }
        return false
    }

    private async existsFile(name: string) {
        try {
            await this.client.send(
                new HeadObjectCommand({
                    Bucket: this.bucket,
                    Key: this.abs(name),
                })
            )
        } catch (e) {
            if (e instanceof NotFound) {
                return false
            } else {
                throw e
            }
        }
        return true
    }

    private async existsDir(dir: string) {
        dir = dir.endsWith('/') ? dir : dir + '/'
        let ls = await this.client.send(
            new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: this.abs(dir),
                MaxKeys: 1,
            })
        )

        if (ls.Contents?.length) {
            return true
        } else {
            return false
        }
    }

    async readFile(name: string): Promise<string> {
        let res = await this.client.send(
            new GetObjectCommand({
                Bucket: this.bucket,
                Key: this.abs(name),
            })
        )
        assert(res.Body != null)
        return res.Body.transformToString('utf-8')
    }

    async writeFile(name: string, data: string | Uint8Array): Promise<void> {
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.abs(name),
                Body: ArrayBuffer.isView(data) ? data : Buffer.from(data, 'utf-8'),
            })
        )
    }

    async mkdir(dir: string): Promise<void> {
        dir = dir.endsWith('/') ? dir : dir + '/'
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.abs(dir),
            })
        )
    }

    async readdir(dir: string): Promise<string[]> {
        dir = dir.endsWith('/') ? dir : dir + '/'

        let names = new Set<string>()

        let ContinuationToken: string | undefined
        while (true) {
            let ls = await this.client.send(
                new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: this.abs(dir),
                    Delimiter: '/',
                    ContinuationToken: ContinuationToken ? ContinuationToken : undefined,
                })
            )

            assert(ls.CommonPrefixes || ls.Contents) // TODO: not a directory, need to add error text

            // process folder names
            if (ls.CommonPrefixes) {
                for (let CommonPrefix of ls.CommonPrefixes) {
                    if (!CommonPrefix.Prefix) continue

                    let folderName = CommonPrefix.Prefix.slice(dir.length, CommonPrefix.Prefix.length - 1)
                    names.add(folderName)
                }
            }

            // process file names
            if (ls.Contents) {
                for (let Content of ls.Contents) {
                    if (!Content.Key || Content.Key == dir) continue

                    let fileName = Content.Key.slice(dir.length)
                    names.add(fileName)
                }
            }

            if (ls.IsTruncated) {
                ContinuationToken = ls.NextContinuationToken
            } else {
                break
            }
        }

        return [...names].sort()
    }

    async rm(name: string): Promise<void> {
        if (name.endsWith('/')) {
            return this.rmDir(name)
        }

        if (await this.existsFile(name)) {
            await this.client.send(
                new DeleteObjectsCommand({
                    Bucket: this.bucket,
                    Delete: {
                        Objects: [{Key: this.abs(name)}],
                        Quiet: true,
                    },
                })
            )
        } else {
            await this.rmDir(name)
        }
    }

    private async rmDir(dir: string): Promise<void> {
        dir = dir.endsWith('/') ? dir : dir + '/'

        let ContinuationToken: string | undefined
        while (true) {
            let ls = await this.client.send(
                new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: this.abs(dir),
                    ContinuationToken: ContinuationToken ? ContinuationToken : undefined,
                })
            )

            if (ls.Contents && ls.Contents.length > 0) {
                await this.client.send(
                    new DeleteObjectsCommand({
                        Bucket: this.bucket,
                        Delete: {
                            Objects: ls.Contents as any,
                            Quiet: true,
                        },
                    })
                )
            }

            if (ls.IsTruncated) {
                ContinuationToken = ls.NextContinuationToken
            } else {
                break
            }
        }
    }

    abs(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }
}

export interface S3Options {
    endpoint?: string
    region?: string
    accessKeyId?: string
    secretAccessKey?: string
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
