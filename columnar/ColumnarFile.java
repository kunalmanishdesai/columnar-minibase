package columnar;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;
import heap.*;

import java.io.IOException;
import java.util.Objects;

public class ColumnarFile {

    private final String name;

    private final Heapfile tidFile;

    private final Heapfile deleteFile;

    private final Heapfile headerFile;

    private final ColumnFile[] columnFiles;

    public int getNumColumns() {
        return numColumns;
    }

    private final  int numColumns;

    public static boolean exists(String name) {
        try {

            PageId pageId = SystemDefs.JavabaseDB.get_file_entry(name + ".hdr");

            return pageId != null;
        } catch (IOException | FileIOException | InvalidPageNumberException | DiskMgrException e) {
            throw new RuntimeException("Error checking if file exisits or not",e);
        }
    }

    public ColumnarFile(
            String name,
            String[] columnNames,
            AttrType[] attrTypes,
            int numColumns
    ) {
        this.name = name;
        this.numColumns = numColumns;
        try {
            headerFile = new Heapfile(name + ".hdr");
            deleteFile = new Heapfile(name+".del");
            tidFile = new Heapfile(name+".tid");
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            throw new RuntimeException("Error creating header file",e);
        }
        columnFiles = new ColumnFile[numColumns];

        for(int i = 0 ; i< numColumns;i++) {
            columnFiles[i] = new ColumnFile(name+"."+i, columnNames[i], attrTypes[i]);
            try {
                headerFile.insertRecord(columnFiles[i].getBytes());
            } catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException | HFException |
                     HFBufMgrException | HFDiskMgrException | IOException e) {
                throw new RuntimeException("Error inserting records",e);
            }
        }
    }

    public ColumnarFile(String name) {
        this.name = name;
        try {
            headerFile = new Heapfile(name + ".hdr");
            deleteFile = new Heapfile(name+".del");
            tidFile = new Heapfile(name+".tid");

            numColumns = headerFile.getRecCnt();
            columnFiles = new ColumnFile[numColumns];

            Scan scan = headerFile.openScan();
            RID rid = new RID();

            for (int i = 0; i < numColumns; i++) {
                columnFiles[i] = new ColumnFile(scan.getNext(rid).getTupleByteArray());
            }

            scan.closescan();
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            throw new RuntimeException("Error creating header file",e);
        } catch (InvalidSlotNumberException | InvalidTupleSizeException e) {
            throw new RuntimeException("Error fetching record count",e);
        }
    }

    public TID insertTuple(int position,byte[] data) {
        Tuple tuple = null;
        try {
            tuple = new Tuple(data);
        } catch (IOException e) {
            throw new RuntimeException("Error getting tuple from ptr",e);
        }

        try {
            TID tid = new TID(numColumns,position);

            for (int i = 0; i< numColumns;i++) {
                ColumnFile columnFile = columnFiles[i];
                RID rid = columnFile.getFile().insertRecord(Utils.insertValue(columnFile.getAttrType(),tuple,i+1).getTupleByteArray());
                tid.setRid(i,rid);
            }

            tidFile.insertRecord(tid.getBytes());

            return tid;

        } catch (SpaceNotAvailableException | HFBufMgrException | InvalidTupleSizeException |
                 InvalidSlotNumberException | HFException | HFDiskMgrException | IOException e) {
            throw new RuntimeException("Error inserting record",e);
        }
    }

    public AttrType[] getAttrTypes() {

        AttrType[] attrTypes = new AttrType[numColumns];
        for (int i = 0; i < numColumns;i++){
            attrTypes[i] = columnFiles[i].getAttrType();
        }
        return attrTypes;
    }

    public String[] getColumnNames() {

        String[] columnNames = new String[numColumns];
        for (int i = 0; i < numColumns;i++){
            columnNames[i] = columnFiles[i].getColumnName();
        }
        return columnNames;
    }

    public int getRecordCount() {
        try {
            return tidFile.getRecCnt();
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException |
                 IOException e) {
            throw new RuntimeException("Error getting record count",e);
        }
    }

    public ColumnFile getColumnFile(int columnNo) {
        return columnFiles[columnNo];
    }

    public Heapfile getTidFile() {
        return tidFile;
    }

    public Heapfile getDeleteFile() {
        return deleteFile;
    }

    public Scan openColumnScan(int column) throws IOException, InvalidTupleSizeException{
        return columnFiles[column].dataFile.openScan();
    }

    public int getColumnNo(String columnName) {
        for(int  i = 0; i < numColumns;i++) {
            if (Objects.equals(columnName, columnFiles[i].getColumnName())) {
                return i;
            }
        }

        return  -1;
    }
}