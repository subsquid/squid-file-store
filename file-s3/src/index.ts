import assert from 'assert'
import path from 'upath'
import {
    DeleteObjectsCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    NotFound,
    PutObjectCommand,
    S3Client,
} from '@aws-sdk/client-s3'
import {assertNotNull} from '@subsquid/util-internal'

export interface S3FsConstructor {
    new (dir: string, bucket: string, options?: S3Options): S3Fs
}

export class S3Fs {
    private client: S3Client

    constructor(private dir: string, private bucket: string, options?: S3Options) {
        this.client = new S3Client(
            options || {
                region: process.env.S3_REGION,
                endpoint: process.env.S3_ENDPOINT,
                credentials: {
                    secretAccessKey: assertNotNull(process.env.S3_SECRET_ACCESS_KEY),
                    accessKeyId: assertNotNull(process.env.S3_ACCESS_KEY_ID),
                },
            }
        )
    }

    async exists(name: string) {
        if (this.isDir(name)) {
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
                    Key: this.path(name),
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
        dir = this.toDir(dir)
        let ls = await this.client.send(
            new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: this.path(dir),
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
                Key: this.path(name),
            })
        )
        assert(res.Body != null)
        return res.Body.transformToString('utf-8')
    }

    async writeFile(name: string, data: string | Uint8Array): Promise<void> {
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.path(name),
                Body: ArrayBuffer.isView(data) ? data : Buffer.from(data, 'utf-8'),
            })
        )
    }

    async mkdir(dir: string): Promise<void> {
        dir = this.toDir(dir)
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.path(dir),
            })
        )
    }

    async readdir(dir: string): Promise<string[]> {
        dir = this.toDir(dir)

        let names = new Set<string>()

        let ContinuationToken: string | undefined
        while (true) {
            let ls = await this.client.send(
                new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: this.path(dir),
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
        if (this.isDir(name)) {
            return this.rmDir(name)
        }

        if (await this.existsFile(name)) {
            await this.client.send(
                new DeleteObjectsCommand({
                    Bucket: this.bucket,
                    Delete: {
                        Objects: [{Key: this.path(name)}],
                        Quiet: true,
                    },
                })
            )
        } else {
            await this.rmDir(name)
        }
    }

    private async rmDir(dir: string): Promise<void> {
        dir = this.toDir(dir)

        let ContinuationToken: string | undefined
        while (true) {
            let ls = await this.client.send(
                new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: this.path(dir),
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

    path(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }

    private toDir(str: string) {
        return this.isDir(str) ? str : str + '/'
    }

    private isDir(str: string) {
        return str.endsWith('/')
    }
}

export interface S3Options {
    endpoint?: string
    region?: string
    accessKeyId?: string
    secretAccessKey?: string
}
