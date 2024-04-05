package bitmap;

import columnar.ColumnFile;
import columnar.Utils;
import global.RID;
import global.TupleOrder;
import global.ValueClass;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class BitmapUtil {

    public static void createBitmap(ColumnFile columnFile) throws Exception {
        String headerFileName = columnFile.getName()+".BM.hdr";
        Heapfile headerFile = new Heapfile(headerFileName);

        HashSet<String> hashSet = new HashSet<>();

        Scan scan = columnFile.getFile().openScan();

        RID rid = new RID();
        Tuple tuple;

        while ((tuple = scan.getNext(rid))!=null) {
            ValueClass value = Utils.getValue(columnFile.getAttrType(),tuple,1);
            String fileName = columnFile.getName() + ".BT." + value + ".hdr";

            if (!hashSet.contains(fileName)) {
                hashSet.add(fileName);
                new BitmapFile(columnFile, value, fileName);
                headerFile.insertRecord(new BitMapFileMeta(fileName, value.toString()).convertToTuple().getTupleByteArray());
            }
        }
    }

    public static List<BitMapFileMeta> getBitmap(String headerFileName) {
        Tuple tuple;
        RID rid = new RID();

        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        FileScan scan = null;
        try {
            scan = new FileScan(headerFileName, BitMapFileMeta.getAttrTypes(),new short[] {30,30}, (short) 2,2,projlist,null);
        } catch (IOException | FileScanException | TupleUtilsException | InvalidRelation e) {
            throw new RuntimeException("Error Scanning",e);
        }

        Sort sort = null;
        try {
            sort = new Sort(BitMapFileMeta.getAttrTypes(), (short) 2, new short[] {30,30}, scan, 2, new TupleOrder(TupleOrder.Ascending), 30, 10);
        } catch (IOException | SortException e) {
            throw new RuntimeException("Error sorting",e);
        }

        List<BitMapFileMeta> BMFileList = new ArrayList<>();

        try{
            while ((tuple = sort.get_next()) != null) {
                BitMapFileMeta bitMapFileMeta = new BitMapFileMeta(tuple.getTupleByteArray());
                BMFileList.add(bitMapFileMeta);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error fetching tuple from sort",e);
        }

        return BMFileList;
    }
}