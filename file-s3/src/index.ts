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
    S3ClientConfig,
} from '@aws-sdk/client-s3'
import {Dest} from '@subsquid/file-store'
import {assertNotNull} from '@subsquid/util-internal'

/**
 * Dest implementation for storing squid data on
 * S3-compatible cloud services.
 *
 * @see https://docs.subsquid.io/basics/store/file-store/s3-dest/
 */
export class S3Dest implements Dest {
    private client: S3Client

    /**
     * Dest implementation for storing squid data on
     * S3-compatible cloud services.
     *
     * @see https://docs.subsquid.io/basics/store/file-store/s3-dest/
     *
     * @param dir - the name of the top level dataset folder
     * @param bucket - the name of the S3 bucket
     * @param optionsOrClient - an S3Client instance or options for its construction
     *
     * The default is to use the environment variables to define the client:
     * ```
     * {
     *     region: process.env.S3_REGION,
     *     endpoint: process.env.S3_ENDPOINT,
     *     credentials: {
     *         secretAccessKey: assertNotNull(process.env.S3_SECRET_ACCESS_KEY),
     *         accessKeyId: assertNotNull(process.env.S3_ACCESS_KEY_ID),
     *     },
     * }
     * ```
     * For details on S3Client
     * @see https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-s3/interfaces/s3clientconfig.html
     */
    constructor(dir: string, bucket: string, options?: S3ClientConfig)
    constructor(dir: string, bucket: string, client: S3Client)
    constructor(private dir: string, private bucket: string, optionsOrClient?: S3ClientConfig | S3Client) {
        if (optionsOrClient instanceof S3Client) {
            this.client = optionsOrClient
        } else {
            this.client = new S3Client(
                optionsOrClient || {
                    region: process.env.S3_REGION,
                    endpoint: process.env.S3_ENDPOINT,
                    credentials: {
                        secretAccessKey: assertNotNull(process.env.S3_SECRET_ACCESS_KEY),
                        accessKeyId: assertNotNull(process.env.S3_ACCESS_KEY_ID),
                    },
                }
            )
        }
    }

    async exists(name: string) {
        if (isDir(name)) {
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
        dir = toDir(dir)

        let prefix = toPrefix(this.path(dir))
        let ls = await this.client.send(
            new ListObjectsV2Command({
                Bucket: this.bucket,
                Prefix: prefix,
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
        dir = toDir(dir)
        await this.client.send(
            new PutObjectCommand({
                Bucket: this.bucket,
                Key: this.path(dir),
            })
        )
    }

    async readdir(dir: string): Promise<string[]> {
        dir = toDir(dir)

        if (!(await this.exists(dir))) return []

        let names = new Set<string>()

        let prefix = toPrefix(this.path(dir))
        let ContinuationToken: string | undefined
        while (true) {
            let ls = await this.client.send(
                new ListObjectsV2Command({
                    Bucket: this.bucket,
                    Prefix: prefix,
                    Delimiter: '/',
                    ContinuationToken: ContinuationToken ? ContinuationToken : undefined,
                })
            )

            assert(ls.CommonPrefixes || ls.Contents) // TODO: not a directory, need to add error text

            // process folder names
            if (ls.CommonPrefixes) {
                for (let CommonPrefix of ls.CommonPrefixes) {
                    if (!CommonPrefix.Prefix) continue

                    let folderName = CommonPrefix.Prefix.slice(prefix.length, CommonPrefix.Prefix.length - 1)
                    names.add(folderName)
                }
            }

            // process file names
            if (ls.Contents) {
                for (let Content of ls.Contents) {
                    if (!Content.Key || Content.Key == prefix) continue

                    let fileName = Content.Key.slice(prefix.length)
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
        if (isDir(name)) {
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
        dir = toDir(dir)

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

    async transact(dir: string, cb: (dest: S3Dest) => Promise<void>): Promise<void> {
        let txDest = new S3Dest(this.path(dir), this.bucket, this.client)
        await cb(txDest)
    }

    path(...paths: string[]) {
        return path.join(this.dir, ...paths)
    }
}

function toDir(str: string) {
    return isDir(str) ? str : str + '/'
}

function isDir(str: string) {
    return str.endsWith('/')
}

function toPrefix(path: string) {
    return path.startsWith('/') ? path.slice(1) : path
}
