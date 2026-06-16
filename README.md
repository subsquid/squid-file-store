# @subsquid/file-store

[![license](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](./LICENSE)

Squid SDK store for writing indexed onchain data to local or S3 files as CSV, Parquet, or JSON.

## Install

```bash
npm i @subsquid/file-store
```

## What it does

`@subsquid/file-store` is a Squid SDK store implementation that persists the output of a squid processor to a filesystem as a dataset. You define tables with typed columns, and the store batches records and writes them to a destination as the processor runs. It supports local folders and Amazon S3 (and S3-compatible storage) as targets, with CSV, Parquet, and JSON as output formats via the companion packages:

- `@subsquid/file-store-csv` for CSV output
- `@subsquid/file-store-parquet` for Parquet output
- `@subsquid/file-store-json` for JSON and JSONL output
- `@subsquid/file-store-s3` for writing to Amazon S3 and S3-compatible storage

This makes it a good fit when you want analytics-ready files or a data-lake dataset instead of a relational database.

## Documentation

Full guides and API reference for the Squid SDK are at [docs.sqd.dev](https://docs.sqd.dev). See the [SDK documentation](https://docs.sqd.dev/en/sdk) for how the store fits into a squid project.

## Community

Questions and discussion happen in the [SQD developers Telegram](https://t.me/HydraDevs).

## License

Licensed under [GPL-3.0-or-later](./LICENSE).
