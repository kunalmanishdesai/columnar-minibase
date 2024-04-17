package commands;

import bufmgr.*;
import columnar.TupleScan;
import columnar.Utils;
import global.SystemDefs;
import heap.Tuple;

import java.io.IOException;

public class DeleteQuery extends ScanCommand {

    private final TupleScan tupleScan;
    public DeleteQuery(String inputCommand) {

        super(inputCommand);
        tupleScan = new TupleScan(columnarFile,outputTupleAttributes);
    }

    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = tupleScan.markTupleDelete()) != null) {
//            System.out.println(Utils.getTupleString(count,tuple,attrTypes));
            count++;
        }

        tupleScan.closeScan();
        System.out.println("Number of tuples deleted: " + count);
    }

    //COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
    public static void main(String[] command) {

        String test = "testdb test1 [A,B,C,D,E] {(C = 2)} 20";
//        String test = command[0];
        DeleteQuery deleteQuery = new DeleteQuery(test);
        deleteQuery.execute();

        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (HashOperationException | PageUnpinnedException | PagePinnedException | PageNotFoundException |
                 BufMgrException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}