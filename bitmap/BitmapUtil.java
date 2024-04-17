package bitmap;

import columnar.ColumnFile;
import columnar.ColumnarFile;
import columnar.Utils;
import global.RID;
import global.ValueClass;
import heap.*;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.IOException;
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
            scan = new Heapfile(headerFileName).openScan();
        } catch (Exception e) {
            throw new RuntimeException("Error Scanning",e);
        }

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

    public static String getBitmapHeader(ColumnFile columnFile, BitmapType bitmapType) {
        return columnFile.getName() + "." + bitmapType.name() + ".BM.hdr";
    }

    public static void printBitmap(ColumnFile columnFile,ColumnarFile columnarFile,BitmapType bitmapType) {

        String bitmapHeader = getBitmapHeader(columnFile,bitmapType);
        List<BitMapFileMeta> bitMapFileMetaList = getBitmap(bitmapHeader);

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

        String bitmapName = getBitmapHeader(columnFile,bitmapType);

        List<BitMapFileMeta> bitMapFileMetaList = getBitmap(bitmapName);
        Set<String> valueAlreadyPresent = bitMapFileMetaList.stream().map(
                bitMapFileMeta -> bitMapFileMeta.getValue().toString()
        ).collect(Collectors.toSet());

        Heapfile headerFile = new Heapfile(bitmapName);

        HashSet<String> hashSet = new HashSet<>();

        Heapfile datafile = columnFile.getFile();
        Scan scan = datafile.openScan();

        Tuple tuple;

        while ((tuple = scan.getNext(startRID))!=null) {

            ValueClass value = Utils.getValue(columnFile.getAttrType(),new Tuple(tuple.getTupleByteArray()),1);
            String fileName = columnFile.getName() + ".BT." + value + ".hdr";

            if (!hashSet.contains(value.toString())) {
                hashSet.add(value.toString());
                BitmapFile bitmapFile = new BitmapFile(fileName,bitmapType);

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

    public static void deleteBitmap(ColumnFile columnFile,BitmapType bitmapType) {

        String headerFileName = getBitmapHeader(columnFile,bitmapType);
        List<BitMapFileMeta> bitMapFileMetaList = getBitmap(headerFileName);

        for(BitMapFileMeta bitMapFileMeta : bitMapFileMetaList) {
            try {
                BitmapFile bitmapFile = new BitmapFile(bitMapFileMeta.bitMapFileName,bitmapType);
                bitmapFile.destroyBitmapFile();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            Heapfile headerFile = new Heapfile(headerFileName);
            headerFile.deleteFile();
        } catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
            throw new RuntimeException(e);
        } catch (InvalidSlotNumberException | InvalidTupleSizeException | FileAlreadyDeletedException e) {
            throw new RuntimeException(e);
        }
    }
}