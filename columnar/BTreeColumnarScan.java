package columnar;

import btree.*;
import global.AttrType;
import global.RID;
import global.TID;
import heap.*;
import index.IndexUtils;
import index.InvalidSelectionException;
import iterator.*;

import java.io.IOException;

public class BTreeColumnarScan {

    private final OutputTupleAttributes outputTupleAttributes;
    private final ColumnarFile columnarFile;

    private final int n_out_flds;

    private final Tuple Jtuple;

    private final Heapfile tidFile;
    private TID tid;

    private final BTFileScan btFileScan;

    private final AttrType type;

    private final boolean index_only;
    public BTreeColumnarScan(ColumnarFile columnarFile, OutputTupleAttributes outputTupleAttributes) {

        this.outputTupleAttributes = outputTupleAttributes;
        this.columnarFile = columnarFile;
        n_out_flds = outputTupleAttributes.getProdSpec().length;
        Jtuple =  new Tuple();
        try {
            TupleUtils.setup_op_tuple(Jtuple, new AttrType[n_out_flds], columnarFile.getAttrTypes(), columnarFile.getNumColumns(), new short[] {30,30,30}, outputTupleAttributes.getProdSpec(), n_out_flds);
        } catch (IOException | TupleUtilsException | InvalidRelation e) {
            throw new RuntimeException("Error setting output tuple",e);
        }

//        CondExpr[] condExprs =
//
//        CondExpr condExpr= new CondExpr(outputTupleAttributes.getCondExprs()[0]);
//        condExpr.next = null;

        tidFile = columnarFile.getTidFile();
        int colNo = outputTupleAttributes.getCondExprs()[0].operand1.symbol.offset-1;


        ColumnFile columnFile = columnarFile.getColumnFile(colNo);
        type = columnFile.getAttrType();

        CondExpr[] btreeCondExpr = new CondExpr[2];

        btreeCondExpr[0] = new CondExpr(outputTupleAttributes.getCondExprs()[0]);
        btreeCondExpr[0].next = null;
        btreeCondExpr[1] = null;

        if (outputTupleAttributes.getCondExprs().length == 2 &&
                outputTupleAttributes.getProdSpec().length == 1 &&
                outputTupleAttributes.getProdSpec()[0].offset ==
                        outputTupleAttributes.getCondExprs()[0].operand1.symbol.offset) {
            index_only = true;
        } else {
            index_only = false;
        }

        try {
            btFileScan = (BTFileScan) IndexUtils.BTree_scan(btreeCondExpr, new BTreeFile(columnFile.getName()+".BT"));
        } catch (IOException | UnknownKeyTypeException | InvalidSelectionException | KeyNotMatchException |
                 UnpinPageException | PinPageException | IteratorException | ConstructPageException |
                 GetFileEntryException e) {
            throw new RuntimeException("Error getting btree file",e);
        }
    }

    public Tuple get_next()
    {
        RID rid;
        KeyDataEntry nextentry;

        try {
            nextentry = btFileScan.get_next();
            while (nextentry != null) {
                if (index_only) {
                    // only need to return the key

                    try {
                        Jtuple.setHdr((short) 1, new AttrType[]{type}, new short[]{30, 30, 30});

                        if (type.attrType == AttrType.attrInteger) {
                            Jtuple.setIntFld(1, ((IntegerKey) nextentry.key).getKey());
                        } else {
                            Jtuple.setStrFld(1, ((StringKey) nextentry.key).getKey());
                        }
                        return Jtuple;
                    } catch (FieldNumberOutOfBoundException | InvalidTupleSizeException | InvalidTypeException e) {
                        throw new RuntimeException("Error scanning index", e);
                    }
                }

                // not index_only, need to return the whole tuple
                rid = ((LeafData) nextentry.data).getData();

                tid = new TID(tidFile.getRecord(rid).getTupleByteArray());
                Tuple oTuple = columnarFile.getTuple(tid);
                if (PredEval.Eval(outputTupleAttributes.getCondExprs(), oTuple, null, columnarFile.getAttrTypes(), null)) {
                    // need projection.java
                    Projection.Project(oTuple, columnarFile.getAttrTypes(), Jtuple, outputTupleAttributes.getProdSpec(), outputTupleAttributes.getProdSpec().length);
                    return Jtuple;
                }

                nextentry = btFileScan.get_next();
            }
        } catch (Exception e) {
            throw new RuntimeException( "IndexScan.java: BTree error",e);
        }
        return null;
    }

    public void closeScan() {
        try {
            btFileScan.DestroyBTreeFileScan();
        }
        catch(Exception e) {
            throw new RuntimeException("Error closing exception", e);
        }
    }
}
