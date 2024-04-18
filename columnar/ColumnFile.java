package columnar;

import bitmap.BitmapType;
import bitmap.BitmapUtil;
import btree.*;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import global.RID;
import global.TID;
import heap.*;

import java.io.IOException;

public class ColumnFile {

    AttrType[] headerRecordAttrType = new AttrType[] {
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrString),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger)
    };

    private final String name;

    public final Heapfile dataFile;

    private final String columnName;

    private final AttrType attrType;

    private boolean hasBtree = false;

    private boolean hasBitmap = false;

    private boolean hasCBitmap = false;

    public RID rid = null;


    public ColumnFile(String columnFileName, String columnName, AttrType attrType) {
        this.name = columnFileName;
        this.columnName = columnName;
        this.attrType = attrType;

        try {
            dataFile = new Heapfile(name);
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            throw new RuntimeException("error creating data file",e);
        }
    }

    public ColumnFile(byte[] data) {
        try {
            Tuple tuple = new Tuple(data);
            name = tuple.getStrFld(1);
            columnName = tuple.getStrFld(2);
            attrType = new AttrType(tuple.getIntFld(3));
            hasBtree = tuple.getIntFld(4) == 1;
            hasBitmap = tuple.getIntFld(5) == 1;
            hasCBitmap = tuple.getIntFld(6) == 1;
            this.rid = rid;

            try {
                dataFile = new Heapfile(name);
            } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
                throw new RuntimeException("error creating data file",e);
            }

        } catch (IOException e) {
            throw new RuntimeException("Error creating tuple",e);
        } catch (FieldNumberOutOfBoundException e) {
            throw new RuntimeException("Error getting field",e);
        }
    }

    public boolean createBtree(Heapfile tidFile) {

        BTreeFile bTreeFile;
        try {
             bTreeFile = new BTreeFile(name+".BT", attrType.attrType, 30, DeleteFashion.FULL_DELETE);
        } catch (GetFileEntryException | ConstructPageException | AddFileEntryException | IOException e) {
            throw new RuntimeException("Error creating btree file");
        }

        try {
            Scan dataFileScan = dataFile.openScan();
            Scan tidFileScan = tidFile.openScan();

            RID scanRID = new RID();
            RID tidRID = new RID();

            Tuple tuple;
            while ((tuple = dataFileScan.getNext(scanRID)) != null) {
                Tuple tuple1 = new Tuple(tuple.getTupleByteArray());
                Tuple tidTuple = tidFileScan.getNext(tidRID);

                TID tid = new TID(tidTuple.getTupleByteArray());
                bTreeFile.insert(Utils.createKey(attrType,tuple1), tidRID);
            }

            tidFileScan.closescan();
            dataFileScan.closescan();
            bTreeFile.close();
        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("error scanning",e);
        } catch (Exception e) {
            throw new RuntimeException("Error creating btree index",e);
        }

        hasBtree = true;

        return true;
    }

    public void deleteBtree() {
        try {
            BTreeFile bTreeFile = new BTreeFile(name+".BT", attrType.attrType, 30, DeleteFashion.FULL_DELETE);
            bTreeFile.destroyFile();
            bTreeFile.close();
        } catch (GetFileEntryException e) {
            throw new RuntimeException(e);
        } catch (ConstructPageException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (AddFileEntryException e) {
            throw new RuntimeException(e);
        } catch (IteratorException e) {
            throw new RuntimeException(e);
        } catch (PinPageException e) {
            throw new RuntimeException(e);
        } catch (UnpinPageException e) {
            throw new RuntimeException(e);
        } catch (FreePageException e) {
            throw new RuntimeException(e);
        } catch (DeleteFileEntryException e) {
            throw new RuntimeException(e);
        } catch (HashEntryNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvalidFrameNumberException e) {
            throw new RuntimeException(e);
        } catch (PageUnpinnedException e) {
            throw new RuntimeException(e);
        } catch (ReplacerException e) {
            throw new RuntimeException(e);
        }
    }

    public RID insert(Tuple tuple) {
        try {
            return dataFile.insertRecord(tuple.getTupleByteArray());
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | SpaceNotAvailableException |
                 HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            throw new RuntimeException("Error inserting record",e);
        }
    }

    public Heapfile getFile() {
       return dataFile;
    }

    public boolean hasBtree() {
        return hasBtree;
    }

    public void setHasBtree(boolean hasBtree) {
        this.hasBtree = hasBtree;
    }

    public void setHasBitmap(boolean hasBitmap) {
        this.hasBitmap = hasBitmap;
    }

    public void setHasCBitmap(boolean hasCBitmap) {
        this.hasCBitmap = hasCBitmap;
    }

    public boolean hasBitmap() {
        return hasBitmap;
    }

    public boolean hasCBitmap() {
        return hasCBitmap;
    }

    public String getName() {
        return name;
    }

    public String getColumnName() {
        return columnName;
    }

    public AttrType getAttrType() {
        return attrType;
    }

    public byte[] getBytes() {

        short[] strSizes = new short[] {
                (short) name.length(),
                (short) columnName.length()
        };
        Tuple tuple = new Tuple();
        try {
            tuple.setHdr((short) 6,headerRecordAttrType,strSizes);
        } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
            throw new RuntimeException("Error setting tuple header",e);
        }

        try {
            tuple.setStrFld(1, name);
            tuple.setStrFld(2,columnName);
            tuple.setIntFld(3,attrType.attrType);
            tuple.setIntFld(4,hasBtree ? 1 : 0);
            tuple.setIntFld(5, hasBitmap? 1 : 0);
            tuple.setIntFld(6, hasCBitmap? 1 : 0);
        } catch (IOException | FieldNumberOutOfBoundException e) {
            throw new RuntimeException("Error setting field",e);
        }

        return tuple.getTupleByteArray();
    }

    public BTreeFile getBtreeFile() {
        try {
            return new BTreeFile(name+".BT");
        } catch (GetFileEntryException | PinPageException| ConstructPageException e) {
            throw new RuntimeException("Error getting btree",e);
        }
    }

    public boolean createBitmap(BitmapType bitmapType,RID startRID) {
        try {
            BitmapUtil.createBitmap(this,bitmapType,startRID);
        } catch (Exception e) {
            throw new RuntimeException("Error creating bitmap file",e);
        }
        return true;
    }

    public boolean deleteRecord(RID rid) {
        try {
            dataFile.deleteRecord(rid);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return true;
    }
}
