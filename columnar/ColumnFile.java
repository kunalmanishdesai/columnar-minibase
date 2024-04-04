package columnar;

import btree.*;
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
            new AttrType(AttrType.attrInteger)
    };

    private final String name;

    public final Heapfile dataFile;

    private final String columnName;

    private final AttrType attrType;

    private boolean hasBtree = false;

    private boolean hasBitmap = false;

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

    public ColumnFile(byte[] data,RID rid) {
        try {
            Tuple tuple = new Tuple(data);
            name = tuple.getStrFld(1);
            columnName = tuple.getStrFld(2);
            attrType = new AttrType(tuple.getIntFld(3));
            hasBtree = tuple.getIntFld(4) == 1;
            hasBitmap = tuple.getIntFld(5) == 1;
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
                tidFileScan.getNext(tidRID);
                bTreeFile.insert(Utils.createKey(attrType,tuple), tidRID);
            }

            tidFileScan.closescan();
            dataFileScan.closescan();
        } catch (InvalidTupleSizeException | IOException e) {
            throw new RuntimeException("error scanning",e);
        } catch (Exception e) {
            throw new RuntimeException("Error creating btree index",e);
        }

        hasBtree = true;

        return true;
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

    public boolean hasBitmap() {
        return hasBitmap;
    }

    public void setHasBitmap(boolean hasBitmap) {
        this.hasBitmap = hasBitmap;
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
            tuple.setHdr((short) 5,headerRecordAttrType,strSizes);
        } catch (IOException | InvalidTypeException | InvalidTupleSizeException e) {
            throw new RuntimeException("Error setting tuple header",e);
        }

        try {
            tuple.setStrFld(1, name);
            tuple.setStrFld(2,columnName);
            tuple.setIntFld(3,attrType.attrType);
            tuple.setIntFld(4,hasBtree ? 1 : 0);
            tuple.setIntFld(5, hasBitmap? 1 : 0);
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
}
