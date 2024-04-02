package columnar;

import global.AttrType;
import global.ValueClass;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;

public class Utils {

    public static ValueClass getValue(AttrType attrType, Tuple tuple, int fldNo){
        try {
            switch (attrType.attrType) {
                case AttrType.attrString: {
                    return new ValueString(tuple.getStrFld(fldNo));
                }

                case AttrType.attrInteger: {
                    return new ValueInt(tuple.getIntFld(fldNo));
                }

                case AttrType.attrReal: {
                    return new ValueFloat(tuple.getFloFld(fldNo));
                }

                default: {
                    throw  new RuntimeException("Attr not found");
                }
            }
        } catch (FieldNumberOutOfBoundException | IOException e) {
            throw new RuntimeException("Error extracting fields", e);
        }
    }

    public static void insertIntoTuple(AttrType attrType, Tuple tuple, int fldNo, Tuple oTuple) {
        try {
            switch (attrType.attrType) {
                case AttrType.attrString: {
                    oTuple.setStrFld(fldNo, tuple.getStrFld(1));
                    return;
                }

                case AttrType.attrInteger: {
                    oTuple.setIntFld(fldNo, tuple.getIntFld(1));
                    return;
                }

                case AttrType.attrReal: {
                    oTuple.setFloFld(fldNo, tuple.getFloFld(1));
                    return;
                }

                default: {
                    throw new RuntimeException("Attr not found");
                }
            }
        } catch (FieldNumberOutOfBoundException | IOException e) {
            throw new RuntimeException("Error extracting fields", e);
        }
    }

    public static Tuple insertValue(AttrType attrType, Tuple tuple, int fldNo){
        try {
            Tuple oTuple = new Tuple();
            oTuple.setHdr((short) 1, new AttrType[] {attrType},new short[] {30});
            switch (attrType.attrType) {
                case AttrType.attrString: {
                    oTuple.setStrFld(1, tuple.getStrFld(fldNo));
                    return oTuple;
                }

                case AttrType.attrInteger: {
                    oTuple.setIntFld(1, tuple.getIntFld(fldNo));
                    return oTuple;
                }

                case AttrType.attrReal: {
                    oTuple.setFloFld(1, tuple.getFloFld(fldNo));
                    return oTuple;
                }

                default: {
                    throw new RuntimeException("Attr not found");
                }
            }
        } catch (FieldNumberOutOfBoundException | IOException e) {
            throw new RuntimeException("Error extracting fields", e);
        } catch (InvalidTupleSizeException | InvalidTypeException e) {
            throw new RuntimeException("Error setting tuple header",e);
        }
    }

    public static String getTupleString(Tuple tuple, AttrType[] attrTypes) {

        String[] strings = new String[attrTypes.length];

        for(int i = 0; i < attrTypes.length;i++) {
            strings[i] = String.format("%-20s", getValue(attrTypes[i],tuple,i+1));
        }

        return String.join("\t", strings);
    }

    public static String getHeaderString(String[] names) {
        String[] formattedColumnNames = new String[names.length];
        for(int i = 0; i < formattedColumnNames.length;i++) {
            formattedColumnNames[i] = String.format("%-20s", names[i]);
        }

        return String.join("\t", formattedColumnNames);
    }
}
