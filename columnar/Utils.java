package columnar;

import btree.IntegerKey;
import btree.KeyClass;
import btree.StringKey;
import global.AttrType;
import global.ValueClass;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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

    public static String getTupleString(int count, Tuple tuple, AttrType[] attrTypes) {

        String[] strings = new String[attrTypes.length+1];

        strings[0] = String.format("%-20s", count);

        for(int i = 0; i < attrTypes.length;i++) {
            strings[i+1] = String.format("%-20s", getValue(attrTypes[i],tuple,i+1));
        }

        return String.join("\t", strings);
    }

    public static String getHeaderString(String[] names) {

        ArrayList<String> formattedColumnNames = new ArrayList<>(Arrays.stream(names).map(str -> String.format("%-20s", str)).toList());
        formattedColumnNames.add(0, String.format("%-20s", "Position"));

        return String.join("\t", formattedColumnNames);
    }

    public static KeyClass createKey(AttrType attrType, Tuple tuple) {

        try {
            switch (attrType.attrType) {
                case AttrType.attrString -> {
                    return new StringKey(tuple.getStrFld(1));
                }

                case AttrType.attrInteger -> {
                    return new IntegerKey(tuple.getIntFld(1));
                }

                default ->
                        throw new IllegalArgumentException("Implementation for key" + attrType.attrType + "not found");
            }
        } catch (FieldNumberOutOfBoundException | IOException e) {
            throw new RuntimeException("Error extracting field from tuple",e);
        }

    }
}
