package bitmap;

import columnar.ValueInt;
import columnar.ValueString;
import global.AttrType;
import global.ValueClass;
import heap.FieldNumberOutOfBoundException;
import heap.Tuple;

import java.io.IOException;

public class BitMapFileMeta {
    String bitMapFileName;
    String value;

    AttrType attrType;

    private final static AttrType[] attrTypes = new AttrType[] {
            new AttrType(AttrType.attrString), // bitmapfileName
            new AttrType(AttrType.attrString), // value
            new AttrType(AttrType.attrInteger)
    };

    public BitMapFileMeta(String bitMapFileName, String value, AttrType attrType) {
        this.bitMapFileName = bitMapFileName;
        this.value = value;
        this.attrType = attrType;
    }

    public BitMapFileMeta(byte[] data) {
        try{
            Tuple tuple = new Tuple(data);
            this.bitMapFileName = tuple.getStrFld(1);
            this.value = tuple.getStrFld(2);
            this.attrType = new AttrType(tuple.getIntFld(3));
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
            tuple.setHdr((short)3, attrTypes , new short[]{(short)bitMapFileName.length(), (short)value.length()});
            tuple.setStrFld(1, bitMapFileName);
            tuple.setStrFld(2, value);
            tuple.setIntFld(3,attrType.attrType);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return tuple;
    }

    public ValueClass getValue() {

        switch (attrType.attrType) {
            case AttrType.attrInteger -> {
                return new ValueInt(Integer.parseInt(value));
            }

            case AttrType.attrString -> {
                return new ValueString(value);
            }

            default -> {
                throw new IllegalArgumentException("Attribute error");
            }
        }
    }
}