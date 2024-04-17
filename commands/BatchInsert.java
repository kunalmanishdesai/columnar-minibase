package commands;

import bufmgr.*;
import columnar.ColumnarFile;
import global.AttrType;
import global.SystemDefs;
import global.TID;
import heap.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class BatchInsert {

    private TID startTID;
    private Short strCount;
    private final int numColumns;

    private final AttrType[] attrTypes;

    private final String[] columnNames;

    private final ColumnarFile columnarFile;

    public void setAttrTypesAndColumnNames(String headerLine) {

        String[] header = headerLine.split("\t");

        strCount = 0;
        for ( int i = 0 ; i< numColumns; i++){
            if (header[i].contains("char"))  {
                attrTypes[i] = new AttrType(AttrType.attrString);
                strCount++;
            } else if (header[i].contains("int")) {
                attrTypes[i] = new AttrType(AttrType.attrInteger);
            } else if (header[i].contains("float")) {
                attrTypes[i] = new AttrType(AttrType.attrReal);
            }
            columnNames[i] = header[i].substring(0,header[i].indexOf(":"));
        }
    }

    public BatchInsert(String name, int numColumns, String headerLine) {

        this.numColumns = numColumns;
        this.attrTypes = new AttrType[numColumns];
        this.columnNames = new String[numColumns];
        setAttrTypesAndColumnNames(headerLine);

        if (ColumnarFile.exists(name)) {
            columnarFile = new ColumnarFile(name);

            if (!Arrays.equals(attrTypes, columnarFile.getAttrTypes())) {
                System.out.println("Column Types not matching");
                System.exit(1);
            }

            if (!Arrays.equals(columnNames, columnarFile.getColumnNames())) {
                System.out.println("Column Names not matching");
                System.exit(1);
            }

        } else {
            columnarFile = new ColumnarFile(name, columnNames, attrTypes, numColumns);
        }
    }

    void execute(BufferedReader br) {
        int position = columnarFile.getRecordCount();
        int startPosition = position;
        String line;

        try {
            while ((line = br.readLine()) != null) {
                insertTuple(position++,startPosition,line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading file",e);
        }

        columnarFile.updateBitmapIndex(startTID);

        System.out.println("Total number of records (" +position + " - " +startPosition+") entered: " + (position-startPosition));
    }

    private void insertTuple(int position,int startPosition,String line) {

        try {
            String[] values = line.split("\t");

            Tuple tuple = new Tuple();

            short[] strSizes = new short[strCount];

            int str = 0;
            for(int i = 0; i < numColumns; i++) {
                if (attrTypes[i].attrType == AttrType.attrString) {
                    strSizes[str] = (short)values[i].length();
                    str++;
                }
            }

            tuple.setHdr((short)numColumns,attrTypes, strSizes);

            for(int i = 0; i < numColumns; i++) {
                if (attrTypes[i].attrType == AttrType.attrString) {
                    tuple.setStrFld(i+1,values[i]);
                } else if (attrTypes[i].attrType == AttrType.attrInteger) {
                    tuple.setIntFld(i+1, Integer.parseInt(values[i]));
                } else if (attrTypes[i].attrType == AttrType.attrReal) {
                    tuple.setFloFld(i+1, Float.parseFloat(values[i]));
                }
            }

            if (position == startPosition) {
                startTID = columnarFile.insertTuple(position,tuple.getTupleByteArray());
            } else {
                columnarFile.insertTuple(position,tuple.getTupleByteArray());
            }
        } catch (FieldNumberOutOfBoundException | InvalidTupleSizeException | IOException | InvalidTypeException e) {
            throw new RuntimeException("error inserting tuple",e);
        }
    }


    public static void main (String[] args) {

        String[] command = args;
//        String[] command = "./src/test_data/5-50000b.txt testdb test1 5 false".split(" ");

        if (command.length != 5) {
            System.out.println("Error: Incorrect number of input values.");
            return;
        }

        // Extract values from the array
        String dataFileName = command[0];
        String columnDBName = command[1];
        String columnarFileName = command[2];
        int numColumns = Integer.parseInt(command[3]);
        boolean flag = Boolean.parseBoolean(command[4]);

        String dbpath = "/tmp/"+columnDBName;

        SystemDefs.MINIBASE_RESTART_FLAG = flag;
        SystemDefs sysdef = new SystemDefs( dbpath, 100000, 800, "Clock" );


        try (BufferedReader br = new BufferedReader( new FileReader(dataFileName))) {

            String headerLine = br.readLine();
            BatchInsert batchInsert = new BatchInsert(columnarFileName, numColumns,headerLine);
            batchInsert.execute(br);

            SystemDefs.JavabaseBM.flushAllPages();

        } catch (IOException e) {
            throw new RuntimeException("Error reading file",e);
        } catch (PageNotFoundException | HashOperationException | BufMgrException | PagePinnedException |
                 PageUnpinnedException e) {
            throw new RuntimeException("Error flushing pages",e);
        }
    }
}
