import {S3Dest} from '@subsquid/file-store-s3';
import fs from 'fs'

async function run() {
    let dest = new S3Dest('s3://csv-store/')
    await dest.readdir('./data').then(console.log)
    await dest.transact('./data', async (d) => {})
}

run()