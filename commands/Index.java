package commands;

import columnar.ColumnarFile;
import global.SystemDefs;

import static global.GlobalConst.NUMBUF;

public class Index {

    public static void main(String [] command) {

        if (command.length != 4) {
            System.out.println("Error: Incorrect number of input values.");
            return;
        }

        String columnDBName = command[0];
        String columnarFileName = command[1];
        String columnName = command[2];
        String indexType = command[3];

        String dbpath = "/tmp/"+columnDBName+".minibase-db";
        String logpath = "/tmp/"+columnDBName+".minibase-log";

        SystemDefs sysdef = new SystemDefs( dbpath, 0, 300, "Clock" );


        ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
        if(indexType.equals("BITMAP")) {

        } else if(indexType.equals("BTREE")) {
//            columnarFile.createBTreeIndex(columnNo-1);
            columnarFile.createBtreeIndex(columnarFile.getColumnNo(columnName));
        } else {
            System.out.println("Error: Incorrect index type.");
        }

        System.out.println(indexType + " Index created on column " + columnName);
    }
}
