/* tslint:disable */
/* eslint-disable */
/*
 * Autogenerated by @creditkarma/thrift-typescript v3.7.6
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
*/
import * as thrift from "thrift";
import * as SplitBlockAlgorithm from "./SplitBlockAlgorithm";
export interface IBloomFilterAlgorithmArgs {
    BLOCK?: SplitBlockAlgorithm.SplitBlockAlgorithm;
}
export class BloomFilterAlgorithm {
    public BLOCK?: SplitBlockAlgorithm.SplitBlockAlgorithm;
    constructor(args?: IBloomFilterAlgorithmArgs) {
        let _fieldsSet: number = 0;
        if (args != null) {
            if (args.BLOCK != null) {
                _fieldsSet++;
                this.BLOCK = args.BLOCK;
            }
            if (_fieldsSet > 1) {
                throw new thrift.Thrift.TProtocolException(thrift.Thrift.TProtocolExceptionType.INVALID_DATA, "Cannot read a TUnion with more than one set value!");
            }
            else if (_fieldsSet < 1) {
                throw new thrift.Thrift.TProtocolException(thrift.Thrift.TProtocolExceptionType.INVALID_DATA, "Cannot read a TUnion with no set value!");
            }
        }
    }
    public static fromBLOCK(BLOCK: SplitBlockAlgorithm.SplitBlockAlgorithm): BloomFilterAlgorithm {
        return new BloomFilterAlgorithm({ BLOCK });
    }
    public write(output: thrift.TProtocol): void {
        output.writeStructBegin("BloomFilterAlgorithm");
        if (this.BLOCK != null) {
            output.writeFieldBegin("BLOCK", thrift.Thrift.Type.STRUCT, 1);
            this.BLOCK.write(output);
            output.writeFieldEnd();
        }
        output.writeFieldStop();
        output.writeStructEnd();
        return;
    }
    public static read(input: thrift.TProtocol): BloomFilterAlgorithm {
        let _fieldsSet: number = 0;
        let _returnValue: BloomFilterAlgorithm | null = null;
        input.readStructBegin();
        while (true) {
            const ret: thrift.TField = input.readFieldBegin();
            const fieldType: thrift.Thrift.Type = ret.ftype;
            const fieldId: number = ret.fid;
            if (fieldType === thrift.Thrift.Type.STOP) {
                break;
            }
            switch (fieldId) {
                case 1:
                    if (fieldType === thrift.Thrift.Type.STRUCT) {
                        _fieldsSet++;
                        const value_1: SplitBlockAlgorithm.SplitBlockAlgorithm = SplitBlockAlgorithm.SplitBlockAlgorithm.read(input);
                        _returnValue = BloomFilterAlgorithm.fromBLOCK(value_1);
                    }
                    else {
                        input.skip(fieldType);
                    }
                    break;
                default: {
                    input.skip(fieldType);
                }
            }
            input.readFieldEnd();
        }
        input.readStructEnd();
        if (_fieldsSet > 1) {
            throw new thrift.Thrift.TProtocolException(thrift.Thrift.TProtocolExceptionType.INVALID_DATA, "Cannot read a TUnion with more than one set value!");
        }
        else if (_fieldsSet < 1) {
            throw new thrift.Thrift.TProtocolException(thrift.Thrift.TProtocolExceptionType.INVALID_DATA, "Cannot read a TUnion with no set value!");
        }
        if (_returnValue !== null) {
            return _returnValue;
        }
        else {
            throw new thrift.Thrift.TProtocolException(thrift.Thrift.TProtocolExceptionType.UNKNOWN, "Unable to read data for TUnion");
        }
    }
}
