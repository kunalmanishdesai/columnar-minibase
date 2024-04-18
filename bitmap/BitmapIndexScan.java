package bitmap;

import columnar.ColumnFile;
import columnar.ColumnarFile;
import columnar.ValueInt;
import columnar.ValueString;
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.ValueClass;
import iterator.CondExpr;

import java.util.List;
import java.util.Objects;

public class BitmapIndexScan {

    private final ColumnarFile columnarFile;

//    private final Scan tidFileScan;
    private BMFileScan scan1 = null;

    private BMFileScan scan0 = null;
    private final String bitmapName;

    private final RID rid = new RID();

    private Integer bit1 = 1;
    private Integer bit0 = 0;


    public BitmapIndexScan(ColumnFile columnFile, ColumnarFile columnarFile, CondExpr condExpr, BitmapType bitmapType) {
        this.bitmapName = BitmapUtil.getBitmapHeader(columnFile,bitmapType);
        this.columnarFile = columnarFile;

        AttrType attrType = columnarFile.getColumnFile(condExpr.operand1.symbol.offset-1).getAttrType();
        AttrOperator attrOperator = condExpr.op;
        ValueClass valueClass = getValue(attrType,condExpr);

        List<BitMapFileMeta> bitMapFileMetaList = BitmapUtil.getBitmap(bitmapName);
        int[] index = getScanIndex(bitMapFileMetaList,valueClass,attrOperator);



//        try {
//            tidFileScan = columnarFile.getTidFile().openScan();
//        } catch (InvalidTupleSizeException | IOException e) {
//            throw new RuntimeException("Error opening tidfile scan",e);
//        }

        if(index[0] != -1) {
            scan1 = new BMFileScan(bitMapFileMetaList.get(index[0]).getName(),bitmapType);
        }

        if(index[1] != -1) {
            scan0 = new BMFileScan(bitMapFileMetaList.get(index[1]).getName(),bitmapType);
        }
    }

    public Boolean get_next() {

        if (scan1 == null && scan0 == null) {
            return null;
        }

        //            Tuple tuple;

        if (scan1 != null) {
            bit1 = scan1.getNext();
        }

        if (scan0 != null) {
            bit0 = scan0.getNext();
        }

        if(bit1 == null || bit0 == null) {
            return null;
        }


        if (bit1 == 1 && bit0 == 0) {
            return true;
        } else {
            return false;
        }


    }

    public int[] getScanIndex(List<BitMapFileMeta> bitMapFileMetaList, ValueClass valueClass, AttrOperator attrOperator) {
       int gteIndex = 0;

       for(BitMapFileMeta bitMapFileMeta: bitMapFileMetaList) {
           if (bitMapFileMeta.getValue().compareTo(valueClass.getValue()) >= 0) {
               break;
           }

           gteIndex+=1;
       }

       int firstIndex = 0;
       int lastIndex = bitMapFileMetaList.size()-1;

        switch (attrOperator.attrOperator) {
            case  AttrOperator.aopEQ -> {

                if (gteIndex > lastIndex) {
                    return new int[]{-1,-1};
                }

                if (!Objects.equals(bitMapFileMetaList.get(gteIndex).getValue().getValue(), valueClass.getValue())) {
                    return new int[]{-1,-1};
                }

                if (gteIndex == 0) {
                    return new int[] {0,-1};
                }

                return new int[] {gteIndex,gteIndex-1};
            }

            case AttrOperator.aopGE -> {

                if (gteIndex == 0) {
                    return new int[] {lastIndex,-1};
                }

                if (gteIndex > lastIndex) {
                    return new int[]{-1, -1};
                }

                return new int[]{-1,gteIndex-1};
            }

            case AttrOperator.aopGT -> {

                if (gteIndex > lastIndex) {
                    return new int[]{-1, -1};
                }

                if(Objects.equals(bitMapFileMetaList.get(gteIndex).getValue().getValue(), valueClass.getValue())) {
                    return new int[] {-1, gteIndex};
                }

                return new int[] {-1, gteIndex-1};
            }

            case AttrOperator.aopLE -> {
                if (gteIndex > lastIndex) {
                    return new int[] {lastIndex,-1};
                }

                return new int[] {gteIndex,-1};
            }

            case AttrOperator.aopLT -> {


                if (gteIndex == 0) {
                    return new int[] {-1,-1};
                }

                if (gteIndex > lastIndex) {
                    return new int[] {lastIndex,-1};
                }

                return new int[] {gteIndex-1,-1};
            }

            case AttrOperator.aopNE -> {

                if( gteIndex < lastIndex && Objects.equals(bitMapFileMetaList.get(gteIndex).getValue().getValue(), valueClass.getValue())) {
                    return new int[] {-1, gteIndex};
                }

                return new int[] {lastIndex,0};
            }
        }

        return null;

    }

    public ValueClass getValue(AttrType attrType, CondExpr condExpr) {
        switch (attrType.attrType) {
            case AttrType.attrInteger -> {
                return new ValueInt(condExpr.operand2.integer);
            }

            case AttrType.attrString -> {
                return new ValueString(condExpr.operand2.string);
            }

            default -> throw new IllegalArgumentException("Wrong attrtype");
        }
    }

    public void closeScan() {
//        tidFileScan.closescan();
        if (scan1 != null) {
            scan1.closeScan();
        }

        if (scan0 != null) {
            scan0.closeScan();
        }
    }
}
