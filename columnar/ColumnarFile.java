package columnar;

import bitmap.BitmapType;
import bitmap.BitmapUtil;
import btree.*;
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

    private Heapfile deleteFile;

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
                columnFiles[i].rid = headerFile.insertRecord(columnFiles[i].getBytes());
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
                columnFiles[i].rid = new RID(rid.pageNo, rid.slotNo);
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
                RID rid = columnFile.insert(Utils.insertValue(columnFile.getAttrType(),tuple,i+1));
                tid.setRid(i,rid);
            }

            RID tidRid = tidFile.insertRecord(tid.getBytes());

            for (int i = 0; i< numColumns;i++) {
                ColumnFile columnFile = columnFiles[i];
                Tuple columnTuple = Utils.insertValue(columnFile.getAttrType(),tuple,i+1);
                if (columnFile.hasBtree()) {
                    BTreeFile bTreeFile = columnFile.getBtreeFile();
                    bTreeFile.insert(Utils.createKey(columnFile.getAttrType(),columnTuple),tidRid);
                    bTreeFile.close();
                }
            }

            return tid;

        } catch (SpaceNotAvailableException | HFBufMgrException | InvalidTupleSizeException |
                 InvalidSlotNumberException | HFException | HFDiskMgrException | IOException e) {
            throw new RuntimeException("Error inserting record", e);
        } catch (IteratorException | ConstructPageException | ConvertException | InsertException |
                 IndexInsertRecException | LeafDeleteException | NodeNotMatchException | LeafInsertRecException |
                 PinPageException | UnpinPageException | DeleteRecException | KeyTooLongException |
                 KeyNotMatchException | IndexSearchException e) {
            throw new RuntimeException("Error inserting into btree",e);
        } catch (HashEntryNotFoundException | InvalidFrameNumberException | PageUnpinnedException | ReplacerException e) {
            throw new RuntimeException("Error closing BTree file",e);
        }
    }

    public Tuple getTuple(TID tid) {

        Tuple oTuple = new Tuple();
        try {
            oTuple.setHdr((short) getNumColumns(), getAttrTypes(), new short[] {30,30,30,30,30,30}) ;
        } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
            throw new RuntimeException("Error setting header for otuple",e);
        }

        try {

        for (int i =0; i< getNumColumns();i++) {

            Tuple tuples = columnFiles[i].getFile().getRecord(tid.getRid(i));

            Tuple columnTuple = new Tuple(tuples.getTupleByteArray());

            ValueClass valueClass = Utils.getValue(columnFiles[i].getAttrType(), columnTuple,1);
//                tid.setRid(i,tid1.getRid(i));
            Utils.insertIntoTuple(getAttrTypes()[i],columnTuple,i+1,oTuple);
            }

        return oTuple;

        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("Error getting tuple",e);
        } catch (Exception e) {
            throw new RuntimeException("Error fetching record from column file", e);
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

    public boolean createBtreeIndex(int colNo) {
        boolean btree = columnFiles[colNo].createBtree(tidFile);
        try {
            headerFile.updateRecord(columnFiles[colNo].rid, new Tuple(columnFiles[colNo].getBytes(), 0,columnFiles[colNo].getBytes().length));
        } catch (Exception e) {
            throw new RuntimeException("Error updating header file",e);
        }
        return btree;
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

    public boolean createBitmap(int colNo, BitmapType bitmapType) {

        ColumnFile columnFile = columnFiles[colNo];

        if (bitmapType == BitmapType.BITMAP) {
            columnFile.setHasBitmap(true);
        }

        if (bitmapType == BitmapType.CBITMAP) {
            columnFile.setHasCBitmap(true);
        }

        try {
            headerFile.updateRecord(columnFile.rid, new Tuple(columnFile.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return columnFile.createBitmap(bitmapType, new RID());
    }

    public boolean updateBitmapIndex(TID startTID) {

        int  i = 0;
        for(ColumnFile columnFile : columnFiles) {
            if ( columnFile.hasBitmap()) {
                columnFile.createBitmap(BitmapType.BITMAP, startTID.getRid(i));
            }

            if ( columnFile.hasCBitmap()) {
                columnFile.createBitmap(BitmapType.CBITMAP, startTID.getRid(i));
            }

            i++;
        }

        return true;
    }

    public void markTupleDeleted(RID rid) {
        try {

            TID tid = new TID(tidFile.getRecord(rid).getTupleByteArray());

            tid.markDeleted();

            byte[] ridBytes = new byte[8];
            rid.writeToByteArray(ridBytes,0);

            deleteFile.insertRecord(ridBytes);

            byte[] tidBytes = tid.getBytes();

            tidFile.updateRecord(rid, new Tuple(tidBytes));
        } catch (Exception e) {
            throw new RuntimeException("Error marking tuple as deleted", e);
        }

    }

    public int purgeAllDeletedTuples() {

        int count = 0;
        try {
            Scan deleteFileScan = deleteFile.openScan();
            RID scanRID = new RID();

            Tuple tuple;
            while ((tuple = deleteFileScan.getNext(scanRID)) !=  null) {

                RID rid = new RID(tuple.getTupleByteArray());
                TID tid = new TID(tidFile.getRecord(rid).getTupleByteArray());

                for (int i=0;i < numColumns;i++) {
                    columnFiles[i].deleteRecord(tid.getRid(i));
                }

                count+=1;
                tidFile.deleteRecord(rid);
            }

            deleteFileScan.closescan();

            deleteFile.deleteFile();
            deleteFile = new Heapfile(this.name+".idr");

            if (count != 0) {
                for(ColumnFile columnFile : columnFiles) {

                    if (columnFile.hasBtree()) {
                        columnFile.deleteBtree();
                        columnFile.createBtree(tidFile);
                    }

                    if ( columnFile.hasBitmap()) {
                        BitmapUtil.deleteBitmap(columnFile, BitmapType.BITMAP);
                        BitmapUtil.createBitmap(columnFile,BitmapType.BITMAP,new RID());
                    }

                    if ( columnFile.hasCBitmap()) {
                        BitmapUtil.deleteBitmap(columnFile, BitmapType.CBITMAP);
                        BitmapUtil.createBitmap(columnFile,BitmapType.CBITMAP,new RID());
                    }
                }
            }

            return count;

        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("Error scanning heap file", e);
        } catch (HFDiskMgrException | HFException | InvalidSlotNumberException | HFBufMgrException e) {
            throw new RuntimeException("Error deleting records", e);
        } catch (Exception e) {
            throw new RuntimeException("Error deleting records ",e);
        }

    }
}
