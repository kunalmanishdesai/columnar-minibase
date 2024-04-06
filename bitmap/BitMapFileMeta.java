package bitmap;

import global.AttrType;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

import java.io.IOException;

public class BitMapFileMeta {
    String bitMapFileName;
    String value;

    private final static AttrType[] attrTypes = new AttrType[] {
            new AttrType(AttrType.attrString), // bitmapfileName
            new AttrType(AttrType.attrString), // value
    };

    public BitMapFileMeta(String bitMapFileName, String value) {
        this.bitMapFileName = bitMapFileName;
        this.value = value;
    }

    public BitMapFileMeta(byte[] data) {
        try{
            Tuple tuple = new Tuple(data);
            this.bitMapFileName = tuple.getStrFld(1);
            this.value = tuple.getStrFld(2);
        } catch (FieldNumberOutOfBoundException | IOException e) {
            throw new RuntimeException("Error getting data from tuple",e);
        }

    }

    public String getName() {
        return bitMapFileName;
    }

    public Tuple convertToTuple() {
        Tuple tuple = new Tuple();
        try {
            tuple.setHdr((short)2, attrTypes , new short[]{(short)bitMapFileName.length(), (short)value.length()});
            tuple.setStrFld(1, bitMapFileName);
            tuple.setStrFld(2, value);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tuple;
    }

    public static AttrType[] getAttrTypes() {
        return attrTypes;
    }

    public String getValue() {
        return value;
    }
}