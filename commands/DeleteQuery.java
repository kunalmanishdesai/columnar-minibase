package commands;

import bufmgr.*;
import columnar.ColumnarFile;
import columnar.TupleScan;
import global.SystemDefs;
import heap.Tuple;
import iterator.OutputTupleAttributes;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DeleteQuery {

    private final TupleScan tupleScan;

    private ColumnarFile columnarFile;

    private OutputTupleAttributes outputTupleAttributes;

    private Boolean purge;

    public DeleteQuery(String inputCommand) {
        scan(inputCommand);
        tupleScan = new TupleScan(columnarFile,outputTupleAttributes);
    }

    void execute() {
        Tuple tuple;
        int count = 0;
        int purgeCount = 0;

        while ((tuple = tupleScan.markTupleDelete()) != null) {
//            System.out.println(Utils.getTupleString(count,tuple,attrTypes));
            count++;
        }

        tupleScan.closeScan();

        System.out.println("Number of tuples deleted: " + count);

        if (purge) {
            purgeCount = columnarFile.purgeAllDeletedTuples();
        }

        System.out.println("Number of tuples purged: " + purgeCount);
    }

    //COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
    public static void main(String[] command) {

        String test = "testdb test1 {(C = 4)} 20 true";

//        System.out.println(command[0]);
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

     void scan(String input) {

         Pattern dbNamePattern = Pattern.compile("(\\w+)\\s+(\\w+)\\s+\\{(.*?)}\\s+(\\d+)\\s+(true|false)");
         Matcher matcher = dbNamePattern.matcher(input);

        if (matcher.find()) {
            // Extract database name, table name, column names, conditions, and integer value
            String columnDBName = matcher.group(1);
            String columnarFileName = matcher.group(2);
            String conditionsStr = matcher.group(3);
            int numBuf = Integer.parseInt(matcher.group(4));
            purge = Boolean.parseBoolean(matcher.group(5));

            String dbpath = "/tmp/"+ columnDBName;
            SystemDefs sysdef = new SystemDefs( dbpath, 0, numBuf, "Clock" );

            columnarFile = new ColumnarFile(columnarFileName);
            outputTupleAttributes = new OutputTupleAttributes(columnarFile,conditionsStr,"");
        } else {
            throw new IllegalArgumentException("Invalid input format");
        }
    }
}