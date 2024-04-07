package bitmap;

import btree.ConstructPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.RID;
import global.SystemDefs;
import heap.*;

import java.io.IOException;

public class BMFileScan {

    private final String filename;
    private Scan bitMapFileScan;

    private BMPage bmPage;

    private RID rid = new RID();

    private BMDataPageInfo bmDataPageInfo;

    private Integer bit;

    private static final int MAX_RECORD_COUNT = 1008;

    public BMFileScan(String filename) {

        try {
            bitMapFileScan = new Heapfile(filename).openScan();
        } catch (HFDiskMgrException | HFException | InvalidTupleSizeException |
                 HFBufMgrException | IOException e) {
            throw new RuntimeException("Error opening heapfile",e);
        }

    }

    public void close() {
        bitMapFileScan.closescan();
    }

    public Integer getNext() {

        if ( (bmPage != null) && (bit = bmPage.getNextBit()) != null) {
            return bit;
        }

        try {

            Tuple tuple = bitMapFileScan.getNext(rid);

            if(tuple == null) {
                return null;
            }

            bmDataPageInfo = new BMDataPageInfo(tuple);

            if (bmPage != null) {
                SystemDefs.JavabaseBM.unpinPage(bmPage.curPage, false);
            }

            bmPage = new BMPage(bmDataPageInfo.pageId);

            bit = bmPage.getNextBit();
            return bit;
        } catch (FieldNumberOutOfBoundException | InvalidTupleSizeException | IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Error getting bmDataPageInfo", e);
        } catch (HashEntryNotFoundException | InvalidFrameNumberException | PageUnpinnedException |
                 ReplacerException e) {
            throw new RuntimeException("Error unpinning page",e);
        } catch (ConstructPageException e) {
            throw new RuntimeException("Error getting bmpage",e);
        }
    }

    public void closeScan() {
        try {
            SystemDefs.JavabaseBM.unpinPage(bmPage.curPage,false);
            bitMapFileScan.closescan();
        } catch (ReplacerException | PageUnpinnedException | HashEntryNotFoundException | InvalidFrameNumberException e) {
            throw new RuntimeException("Error unpinning page",e);
        }
    }
}