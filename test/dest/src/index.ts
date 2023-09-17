import {S3Dest} from '@subsquid/file-store-s3';

async function run() {
    let dest = new S3Dest('./', 'csv-store')
    await dest.readdir('./data').then(console.log)
}

run()