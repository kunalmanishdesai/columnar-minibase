package columnar;

import bitmap.BitmapIndexScan;
import index.BooleanScan;
import bitmap.BitmapType;
import global.AttrType;
import global.RID;
import global.TID;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;

public class BTIndexScan {

    private final OutputTupleAttributes outputTupleAttributes;
    private final ColumnarFile columnarFile;
    private final  ArrayList<BooleanScan> scans;

    private final Scan tidFileScan;
    private final CondExpr[] condExprs;

    private RID rid = new RID();
    private final ArrayList<RID> rids = new ArrayList<>();

    private final int n_out_flds;

    private final Tuple Jtuple;

    private TID tid;

    private final AttrType[] condAttr;

    public BTIndexScan(ColumnarFile columnarFile, OutputTupleAttributes outputTupleAttributes, BitmapType bitmapType) {

        this.outputTupleAttributes = outputTupleAttributes;
        this.columnarFile = columnarFile;
        n_out_flds = outputTupleAttributes.getProdSpec().length;
        Jtuple =  new Tuple();
        try {
            TupleUtils.setup_op_tuple(Jtuple, new AttrType[n_out_flds], columnarFile.getAttrTypes(), columnarFile.getNumColumns(), new short[] {30,30,30,30,30,30}, outputTupleAttributes.getProdSpec(), n_out_flds);
        } catch (IOException | TupleUtilsException | InvalidRelation e) {
            throw new RuntimeException("Error setting output tuple",e);
        }

        try {
            tidFileScan = columnarFile.getTidFile().openScan();
        } catch (IOException | InvalidTupleSizeException e) {
            throw new RuntimeException("Error creating scans",e);
        }


        scans = new ArrayList<>();
        condAttr = outputTupleAttributes.getOutputAttrs();
        condExprs = outputTupleAttributes.getCondExprs();

        for(int i = 0; i < condExprs.length;i++) {

            CondExpr condExpr = condExprs[i];

            while (condExpr != null) {

                rids.add(new RID());

                int colNo = condExpr.operand1.symbol.offset-1;
                scans.add(getScan(colNo, condExpr, bitmapType));
                condExpr = condExpr.next;
            }

        }

    }

    public Tuple get_next() {

        while(true) {
            try{
                Tuple tidTuple = tidFileScan.getNext(rid);

                if (tidTuple == null) {
                    return null;
                }

                tid = new TID(tidTuple.getTupleByteArray());

                boolean isValidTupleAND = true;

                int counter = 0;
                for(int i = 0; i< condExprs.length-1; i++) {

                    CondExpr condExpr = condExprs[i];

                    boolean isValidTupleOR = false;

                    Boolean isValidTuple = null;
                    while (condExpr != null) {
//                        Tuple tuple = new Tuple(scans.get(counter).getNext().getTupleByteArray());
                        isValidTuple = scans.get(counter).get_next();

                        if (isValidTuple == null) {
                            isValidTupleOR = false;
                        } else {
                            isValidTupleOR = (isValidTupleOR || isValidTuple);
                        }

                        condExpr = condExpr.next;
                        counter++;
                    }

                    if (isValidTuple == null) {
                        return null;
                    }

                    isValidTupleAND = isValidTupleAND && isValidTupleOR;
                }

                if (isValidTupleAND && !tid.isDeleted()) {
                    Tuple oTuple = columnarFile.getTuple(tid);
                    Projection.Project(oTuple, columnarFile.getAttrTypes(),  Jtuple, outputTupleAttributes.getProdSpec(), n_out_flds);
                    return Jtuple;
                }

            } catch (InvalidTupleSizeException | IOException e) {
                throw new RuntimeException("Error fetching next tuple",e);
            } catch (UnknowAttrType | FieldNumberOutOfBoundException e) {
                throw new RuntimeException("Error in evaluating",e);
            } catch (WrongPermat e) {
                throw new RuntimeException("error projection",e);
            }
        }

    }


    public void closeScan() {
        for(BooleanScan scan: scans) {
            scan.close();
        }

        tidFileScan.closescan();
    }

    public BooleanScan getScan(int colNo, CondExpr condExpr,BitmapType bitmapType) {

        ColumnFile columnFile =  columnarFile.getColumnFile(colNo);

        if (columnFile.hasCBitmap() && bitmapType == BitmapType.CBITMAP) {
            return new BitmapIndexScan(columnFile, columnarFile, condExpr, BitmapType.CBITMAP);
        }

        if (columnFile.hasBitmap() && bitmapType == BitmapType.BITMAP) {
            return new BitmapIndexScan(columnFile, columnarFile, condExpr, BitmapType.BITMAP);
        }

        return new BooleanColumnScan(columnFile, columnarFile, condExpr);
    }
}
