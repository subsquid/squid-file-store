# Change Log - @subsquid/file-store-parquet

This log was last generated on Mon, 27 Mar 2023 20:14:02 GMT and should not be manually modified.

## 0.5.2
Mon, 27 Mar 2023 20:14:02 GMT

### Patches

- add binary type

## 0.5.1
Mon, 27 Mar 2023 19:43:20 GMT

### Patches

- fix missing type length parameter

## 0.5.0
Mon, 27 Mar 2023 19:31:45 GMT

### Minor changes

- add length option for string type

## 0.4.0
Wed, 22 Mar 2023 10:57:33 GMT

### Minor changes

- allow to use `number` as value type for Int64

### Patches

- fix incorrect type inference of nullable column

## 0.3.0
Wed, 15 Mar 2023 09:08:21 GMT

### Minor changes

- add Decimal type
- add JSON and BSON types
- add inline docs

### Patches

- reduce memory usage on serialization
- fix incorrect ConvertedType for Int16

## 0.2.4
Fri, 10 Mar 2023 08:07:56 GMT

### Patches

- fix incorrect default row size value

## 0.2.3
Wed, 01 Mar 2023 14:42:25 GMT

### Patches

- add missing 'lzo' package dependency
- import compression packages only when needed

## 0.2.2
Wed, 01 Mar 2023 14:12:41 GMT

### Patches

- add "thrift" to list of publishing folders

## 0.2.1
Tue, 28 Feb 2023 14:16:26 GMT

### Patches

- add missing compressions

## 0.2.0
Mon, 27 Feb 2023 19:04:13 GMT

### Minor changes

- use own writing instead of arrow

## 0.1.1
Thu, 16 Feb 2023 10:44:40 GMT

_Version update only_

## 0.1.0
Wed, 01 Feb 2023 20:37:01 GMT

### Minor changes

- change types naming and wrap them

## 0.0.1
Wed, 01 Feb 2023 11:09:12 GMT

### Patches

- change `Uint32` typescript type to `number`

