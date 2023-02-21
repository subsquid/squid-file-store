import { compressToBuffer, maxCompressedLength } from './compressor';

function isNode() {
  if (typeof process === 'object') {
    if (typeof process.versions === 'object') {
      if (typeof process.versions.node !== 'undefined') {
        return true;
      }
    }
  }
  return false;
}

function isUint8Array(object: any) {
  return object instanceof Uint8Array && (!isNode() || !Buffer.isBuffer(object));
}

function isArrayBuffer(object: any) {
  return object instanceof ArrayBuffer;
}

function isBuffer(object: any) {
  if (!isNode()) {
    return false;
  }
  return Buffer.isBuffer(object);
}

const TYPE_ERROR_MSG = 'Argument compressed must be type of ArrayBuffer, Buffer, or Uint8Array';

export function compress(uncompressed: Buffer): Buffer;
export function compress(uncompressed: Uint8Array): Uint8Array;
export function compress(uncompressed: ArrayBuffer): ArrayBuffer;
export function compress(uncompressed: any) {
  if (!isUint8Array(uncompressed) && !isArrayBuffer(uncompressed) && !isBuffer(uncompressed)) {
    throw new TypeError(TYPE_ERROR_MSG);
  }
  let uint8Mode = false;
  let arrayBufferMode = false;
  let buffer: Buffer;
  if (isUint8Array(uncompressed)) {
    uint8Mode = true;
    buffer = Buffer.from(uncompressed.buffer, uncompressed.byteOffset, uncompressed.byteLength);
  } else if (isArrayBuffer(uncompressed)) {
    arrayBufferMode = true;
    buffer = Buffer.from(uncompressed);
  } else {
    buffer = uncompressed;
  }

  const maxLength = maxCompressedLength(buffer);
  const target: Buffer = Buffer.alloc(maxLength);
  const length = compressToBuffer(buffer, target);
  const array = target.buffer.slice(0, length);

  if (uint8Mode) {
    return new Uint8Array(array);
  } else if (arrayBufferMode) {
    return array;
  } else {
    return Buffer.from(array);
  }
}
