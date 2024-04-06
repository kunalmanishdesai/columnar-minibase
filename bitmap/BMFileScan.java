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

    private Scan bitMapFileScan;

    private BMPage bmPage;

    private int position = 1009;

    private RID rid = new RID();

    private BMDataPageInfo bmDataPageInfo;

    private int pinned = 0;

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

        if ( position > MAX_RECORD_COUNT) {
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
            } catch (FieldNumberOutOfBoundException | InvalidTupleSizeException | IOException e) {
                // TODO Auto-generated catch block
              throw new RuntimeException("Error getting bmDataPageInfo", e);
            } catch (HashEntryNotFoundException | InvalidFrameNumberException | PageUnpinnedException |
                     ReplacerException e) {
                throw new RuntimeException("Error unpinning page",e);
            } catch (ConstructPageException e) {
                throw new RuntimeException("Error getting bmpage",e);
            }

            if(rid == null) return null;
            position = 0;  
        }

        Integer bit;
        try {
            bit = bmPage.getBit(position);
        } catch ( Exception e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("error getting bit",e);
        }
        position++;
        return bit;
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