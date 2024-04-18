package commands;

import bitmap.BitmapType;
import bitmap.BitmapUtil;
import bufmgr.*;
import columnar.ColumnarFile;
import global.SystemDefs;

import java.io.IOException;

public class Index {

    public static void main(String [] command) {

//        command = "testdb test1 C BTREE".split(" ");
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
        int colNo = columnarFile.getColumnNo(columnName);
        if(indexType.equals("BITMAP")) {
            columnarFile.createBitmap(colNo, BitmapType.BITMAP);
        } else if (indexType.equals("CBITMAP")) {
            columnarFile.createBitmap(colNo, BitmapType.CBITMAP);
        } else if(indexType.equals("BTREE")) {
            columnarFile.createBtreeIndex(colNo);
        } else  if (indexType.equals("PBITMAP")) {
            BitmapUtil.printBitmap(columnarFile.getColumnFile(colNo),columnarFile,BitmapType.BITMAP);
        } else  if (indexType.equals("PCBITMAP")) {
            BitmapUtil.printBitmap(columnarFile.getColumnFile(colNo),columnarFile,BitmapType.CBITMAP);
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
