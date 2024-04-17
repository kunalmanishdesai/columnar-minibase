package bitmap;

import columnar.ColumnFile;
import columnar.ColumnarFile;
import columnar.Utils;
import global.RID;
import global.ValueClass;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.FldSpec;
import iterator.RelSpec;

import java.util.*;
import java.util.stream.Collectors;

public class BitmapUtil {

    public static List<BitMapFileMeta> getBitmap(String headerFileName) {
        Tuple tuple;
        RID rid = new RID();

        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        Scan scan = null;
        try {
//            scan = new FileScan(headerFileName, BitMapFileMeta.getAttrTypes(),new short[] {30,30}, (short) 2,2,projlist,null);
            scan = new Heapfile(headerFileName).openScan();
        } catch (Exception e) {
            throw new RuntimeException("Error Scanning",e);
        }

//        Sort sort = null;
//        try {
//            sort = new Sort(BitMapFileMeta.getAttrTypes(), (short) 2, new short[] {30,30}, scan, 2, new TupleOrder(TupleOrder.Ascending), 30, 10);
//        } catch (IOException | SortException e) {
//            throw new RuntimeException("Error sorting",e);
//        }

        List<BitMapFileMeta> BMFileList = new ArrayList<>();


        try{
            while ((tuple = scan.getNext(rid)) != null) {
                BitMapFileMeta bitMapFileMeta = new BitMapFileMeta(tuple.getTupleByteArray());
                BMFileList.add(bitMapFileMeta);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error fetching tuple from sort",e);
        }
        scan.closescan();

        BMFileList.sort(Comparator.comparing(bitMapFileMeta -> bitMapFileMeta.getValue(), (value1, value2) -> value1.compareTo(value2.getValue())));
        return BMFileList;
    }

    public static void printBitmap(String headerFileName, ColumnarFile columnarFile,BitmapType bitmapType) {
        List<BitMapFileMeta> bitMapFileMetaList = getBitmap(headerFileName);

        List<BMFileScan> scans = bitMapFileMetaList.stream()
                .map(bitMapFileMeta -> new BMFileScan(bitMapFileMeta.getName(),bitmapType))
                .toList();

        System.out.printf("%-14s | ", "position");
        for (BitMapFileMeta bitMapFileMeta : bitMapFileMetaList) {
            String value = bitMapFileMeta.value;
            System.out.printf("%-14s | ", value); // Adjust the spacing as needed
        }

        System.out.println();

        boolean stopFlag = false;
        int position = 0;

        while (position < columnarFile.getRecordCount()) {
            Integer bit;
            System.out.printf("%-14s | ", position);

            for (BMFileScan scan : scans) {
                bit = scan.getNext();

                if (bit == null) {
                    stopFlag = true;
                    break;
                }

                System.out.printf("%-14s | ", bit); // Adjust the spacing as needed
            }

            position += 1;
            if (stopFlag) {
                if (position != columnarFile.getRecordCount()) {
                    System.out.println("broke in between" + position);
                }
                break;
            }

            System.out.println();
        }

        for (BMFileScan scan : scans) {
            scan.closeScan();
        }
    }

    public static void createBitmap(ColumnFile columnFile,BitmapType bitmapType, RID startRID) throws Exception {

        String bitmapName = columnFile.getName()+".BM.hdr";

        List<BitMapFileMeta> bitMapFileMetaList = getBitmap(bitmapName);
        Set<String> valueAlreadyPresent = bitMapFileMetaList.stream().map(
                bitMapFileMeta -> bitMapFileMeta.getValue().toString()
        ).collect(Collectors.toSet());

        String headerFileName = columnFile.getName()+".BM.hdr";
        Heapfile headerFile = new Heapfile(headerFileName);

        HashSet<String> hashSet = new HashSet<>();

        Heapfile datafile = columnFile.getFile();
        Scan scan = datafile.openScan();

        Tuple tuple;

        while ((tuple = scan.getNext(startRID))!=null) {

            ValueClass value = Utils.getValue(columnFile.getAttrType(),new Tuple(tuple.getTupleByteArray()),1);
            String fileName = columnFile.getName() + ".BT." + value + ".hdr";

            if (!hashSet.contains(value.toString())) {
                hashSet.add(value.toString());
                BitmapFile bitmapFile = new BitmapFile(columnFile, value, fileName,bitmapType);

                if (!valueAlreadyPresent.contains(value.toString())) {
                    bitmapFile.accessColumn(columnFile,value,startRID);
                    headerFile.insertRecord(new BitMapFileMeta(fileName, value.toString(),columnFile.getAttrType()).convertToTuple().getTupleByteArray());
                    valueAlreadyPresent.add(value.toString());
                } else {
                    bitmapFile.accessColumn(columnFile,value,startRID);
                }
            }
        }
    }
}