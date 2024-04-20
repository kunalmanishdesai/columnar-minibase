package columnar;

import global.*;
import heap.*;
import index.BooleanScan;
import iterator.*;

import java.io.IOException;

public class BooleanColumnScan implements BooleanScan {
    private final Scan scan;
    private RID rid = new RID();
    private final CondExpr[] condExpr = new CondExpr[2];
    private final AttrType attrType;

    public BooleanColumnScan(ColumnFile columnFile, ColumnarFile columnarFile, CondExpr condExpr) {

        attrType = columnarFile.getColumnFile(condExpr.operand1.symbol.offset-1).getAttrType();

        this.condExpr[0] = new CondExpr(condExpr);
        this.condExpr[0].operand1.symbol.offset = 1;
        this.condExpr[0].next = null;
        this.condExpr[1] = null;


        try {
            scan = columnFile.getFile().openScan();
        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("Error opening scan",e);
        }
    }


    public Boolean get_next() {

        try{
            Tuple tuple = new Tuple(scan.getNext(rid).getTupleByteArray());
            return PredEval.Eval(condExpr,tuple,null, new AttrType[] {attrType}, null);
        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("Error fetching next tuple",e);
        } catch (UnknowAttrType | FieldNumberOutOfBoundException | PredEvalException | InvalidTypeException e) {
            throw new RuntimeException("Error in evaluating",e);
        }
    }

    public void close() {
        scan.closescan();
    }
}
