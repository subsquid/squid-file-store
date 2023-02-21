import { ParquetSchema } from './schema';
import { ParquetEnvelopeWriter, ParquetWriter } from './writer';
import { writeFileSync } from 'fs';

let schema = new ParquetSchema({
  name: { type: 'UTF8' },
  quantity: { type: 'INT64', optional: true },
  price: { type: 'DOUBLE' },
});

let res: Buffer[] = [];

let write = new ParquetWriter(
  schema,
  new ParquetEnvelopeWriter(
    schema,
    async buf => {
      res.push(buf);
    },
    async () => {
      writeFileSync('test.parquet', Buffer.concat(res));
    },
    0,
    {}
  ),
  {}
);

async function test() {
  await write.appendRow({
    name: 'apple',
    quantity: 10,
    price: 23.5,
  });
  await write.close();
}

test();
