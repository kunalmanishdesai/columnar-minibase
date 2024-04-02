package columnar;

import global.AttrType;
import global.RID;
import global.TID;
import heap.*;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.PredEval;
import iterator.Projection;

import java.io.IOException;

public class TupleScan {

    private final TID tid;
    private final RID rid = new RID();

//    private final FldSpec[] prjSpec;
//
//    private final AttrType[] outPutAttr;
//    private final CondExpr[] condExpr;
    private final ColumnarFile columnarFile;

    private final Scan tidFileScan;

    private final Scan[] columnScans;


    public TupleScan(ColumnarFile columnarFile, AttrType[] outputAttr, FldSpec[] prjSpec, CondExpr[] condExpr) {
        this.columnarFile = columnarFile;

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

//                if (PredEval.Eval(, tuple1, null, _in1, null) == true){
//                    Projection.Project(tuple1, _in1,  Jtuple, perm_mat, nOutFlds);
//                    return  Jtuple;
//                }

                if (tid1.isDeleted()) {
                    continue;
                }

                return oTuple;

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
