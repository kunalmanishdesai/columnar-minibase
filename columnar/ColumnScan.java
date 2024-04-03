package columnar;

import global.AttrType;
import global.RID;
import global.TID;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;

public class ColumnScan {

    private final OutputTupleAttributes outputTupleAttributes;
    private final ColumnarFile columnarFile;
    private final  ArrayList<Scan> scans;

    private final Scan tidFileScan;
    private final CondExpr[] condExprs;

    private RID rid = new RID();
    private final ArrayList<RID> rids = new ArrayList<>();

    private final int n_out_flds;

    private final Tuple Jtuple;

    private TID tid;

    private final AttrType[] condAttr;

    public ColumnScan(ColumnarFile columnarFile, OutputTupleAttributes outputTupleAttributes) {

        this.outputTupleAttributes = outputTupleAttributes;
        this.columnarFile = columnarFile;
        n_out_flds = outputTupleAttributes.getProdSpec().length;
        Jtuple =  new Tuple();
        try {
            TupleUtils.setup_op_tuple(Jtuple, new AttrType[n_out_flds], columnarFile.getAttrTypes(), columnarFile.getNumColumns(), new short[] {30,30,30}, outputTupleAttributes.getProdSpec(), n_out_flds);
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
                try {
                    rids.add(new RID());
                    scans.add(columnarFile.openColumnScan(condExpr.operand1.symbol.offset-1));
                } catch (IOException | InvalidTupleSizeException e) {
                    throw new RuntimeException("Error creating scans",e);
                }

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

                    while (condExpr != null) {
                        Tuple tuple = new Tuple(scans.get(counter).getNext(rids.get(counter)).getTupleByteArray());

                        CondExpr[] tempCond = new CondExpr[2];
                        tempCond[0] = new CondExpr(condExpr);
                        tempCond[0].next = null;
                        tempCond[0].operand1.symbol.offset = 1;
                        tempCond[1] = null;
                        
                        AttrType[] tempAttr = new AttrType[] {condAttr[counter]};

                        isValidTupleOR = (isValidTupleOR || PredEval.Eval(tempCond,tuple,null, tempAttr, null));
                        condExpr = condExpr.next;

                        counter++;
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
            } catch (UnknowAttrType | FieldNumberOutOfBoundException | PredEvalException | InvalidTypeException e) {
                throw new RuntimeException("Error in evaluating",e);
            } catch (WrongPermat e) {
                throw new RuntimeException("error projection",e);
            }
        }

    }


    public void closeScan() {
        for(Scan scan: scans) {
            scan.closescan();
        }

        tidFileScan.closescan();
    }
}
