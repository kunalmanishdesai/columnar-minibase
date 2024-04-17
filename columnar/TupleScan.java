package columnar;

import global.AttrType;
import global.RID;
import global.TID;
import heap.*;
import iterator.*;

import java.io.IOException;

public class TupleScan {

    private final TID tid;
    private final RID rid = new RID();

    private final ColumnarFile columnarFile;

    private final OutputTupleAttributes outputTupleAttributes;

    private final int n_out_flds;

    private final Tuple Jtuple;

    private final Scan tidFileScan;

    private final Scan[] columnScans;


    //AttrType[] outputAttr, FldSpec[] prjSpec, CondExpr[] condExpr
    public TupleScan(ColumnarFile columnarFile, OutputTupleAttributes outputTupleAttributes) {
        this.columnarFile = columnarFile;
        this.outputTupleAttributes = outputTupleAttributes;

        n_out_flds = outputTupleAttributes.getProdSpec().length;
        Jtuple =  new Tuple();
        try {
            TupleUtils.setup_op_tuple(Jtuple, new AttrType[n_out_flds], columnarFile.getAttrTypes(), columnarFile.getNumColumns(), new short[] {30,30,30}, outputTupleAttributes.getProdSpec(), n_out_flds);
        } catch (IOException | TupleUtilsException | InvalidRelation e) {
            throw new RuntimeException("Error setting output tuple",e);
        }

        try {
            tidFileScan = columnarFile.getTidFile().openScan();
            columnScans = new Scan[columnarFile.getNumColumns()];

            for(int i = 0; i < columnarFile.getNumColumns();i++) {
                columnScans[i] = columnarFile.openColumnScan(i);
            }

            tid = new TID(columnarFile.getNumColumns(), 0);
        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("Error opening scans",e);
        }
    }

    public Tuple get_next() {

        while(true) {
            try{
                Tuple tuple= tidFileScan.getNext(rid);

                if (tuple == null) {
                    return null;
                }

                TID tid1 = new TID(tuple.getTupleByteArray());

                Tuple oTuple = new Tuple();
                try {
                    oTuple.setHdr((short) columnarFile.getNumColumns(), columnarFile.getAttrTypes(), new short[] {30,30,30}) ;
                } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
                    throw new RuntimeException("Error setting header for otuple",e);
                }

                for (int i =0; i< columnarFile.getNumColumns();i++) {

                    Tuple columnTuple = new Tuple(columnScans[i].getNext(tid.getRid(i)).getTupleByteArray());
//                tid.setRid(i,tid1.getRid(i));
                    Utils.insertIntoTuple(columnarFile.getAttrTypes()[i],columnTuple,i+1,oTuple);
                }

                try {
                    if (!tid1.isDeleted() && PredEval.Eval(outputTupleAttributes.getCondExprs(), oTuple, null, columnarFile.getAttrTypes(), null)){
                        Projection.Project(oTuple, columnarFile.getAttrTypes(),  Jtuple, outputTupleAttributes.getProdSpec(), n_out_flds);
                        return  Jtuple;
                    }
                } catch (UnknowAttrType | FieldNumberOutOfBoundException | PredEvalException e) {
                    throw new RuntimeException("Error checking condition",e);
                } catch (InvalidTypeException | WrongPermat e) {
                    throw new RuntimeException("Error projection tuple",e);
                }

            } catch (InvalidTupleSizeException | IOException e) {
                throw new RuntimeException("Error fetching next tuple",e);
            }
        }

    }

    public Tuple markTupleDelete() {

        while(true) {
            try{
                Tuple tuple= tidFileScan.getNext(rid);

                if (tuple == null) {
                    return null;
                }

                TID tid1 = new TID(tuple.getTupleByteArray());

                Tuple oTuple = new Tuple();
                try {
                    oTuple.setHdr((short) columnarFile.getNumColumns(), columnarFile.getAttrTypes(), new short[] {30,30,30}) ;
                } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
                    throw new RuntimeException("Error setting header for otuple",e);
                }

                for (int i =0; i< columnarFile.getNumColumns();i++) {

                    Tuple columnTuple = new Tuple(columnScans[i].getNext(tid.getRid(i)).getTupleByteArray());
                    Utils.insertIntoTuple(columnarFile.getAttrTypes()[i],columnTuple,i+1,oTuple);
                }

                try {
                    if (!tid1.isDeleted() && PredEval.Eval(outputTupleAttributes.getCondExprs(), oTuple, null, columnarFile.getAttrTypes(), null)){
                        columnarFile.markTupleDeleted(rid);
                        Projection.Project(oTuple, columnarFile.getAttrTypes(),  Jtuple, outputTupleAttributes.getProdSpec(), n_out_flds);
                        return  Jtuple;
                    }
                } catch (UnknowAttrType | FieldNumberOutOfBoundException | PredEvalException e) {
                    throw new RuntimeException("Error checking condition",e);
                } catch (InvalidTypeException | WrongPermat e) {
                    throw new RuntimeException("Error projection tuple",e);
                }

            } catch (InvalidTupleSizeException | IOException e) {
                throw new RuntimeException("Error fetching next tuple",e);
            }
        }

    }

    public void closeScan() {
        tidFileScan.closescan();
        for(int i = 0; i < columnarFile.getNumColumns();i++){
            columnScans[i].closescan();
        }
    }
}
