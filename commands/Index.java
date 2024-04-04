package commands;

import bufmgr.*;
import columnar.ColumnarFile;
import global.SystemDefs;

import java.io.IOException;

public class Index {

    public static void main(String [] command) {

//        command = "testdb test1 A BTREE".split(" ");
        if (command.length != 4) {
            System.out.println("Error: Incorrect number of input values.");
            return;
        }

        String columnDBName = command[0];
        String columnarFileName = command[1];
        String columnName = command[2];
        String indexType = command[3];

        String dbpath = "/tmp/"+ columnDBName;

        SystemDefs sysdef = new SystemDefs( dbpath, 0, 300, "Clock" );

        ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
        if(indexType.equals("BITMAP")) {

        } else if(indexType.equals("BTREE")) {
//            columnarFile.createBTreeIndex(columnNo-1);
            columnarFile.createBtreeIndex(columnarFile.getColumnNo(columnName));
        } else {
            System.out.println("Error: Incorrect index type.");
        }

        try {
            SystemDefs.JavabaseBM.flushAllPages();
        } catch (HashOperationException | PageUnpinnedException | PagePinnedException | PageNotFoundException |
                 BufMgrException | IOException e) {
            throw new RuntimeException("Error flushing pages",e);
        }

        System.out.println(indexType + " Index created on column " + columnName);
    }
}
