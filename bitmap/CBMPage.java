package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

import java.io.IOException;

public class CBMPage extends HFPage implements BMPageInterface {

    public int currentReadSlot = 0;

    public int slotCount = 1;

    public static final int SLOT_SIZE = 1+4;
    public static final int IP_FIXED_DATA = 2 + 4;
    private static final int SLOTS_USED_POS = 0;
    private static final int CUR_PAGE_POS = 2;

    private static final int PAGE_SIZE = 1024;

    /**
     * No. of slots used
     */
    private short slotsUsed;

    /**
     * PageId of this page
     */
    protected PageId curPage = new PageId();

    public CBMPage() {
        try {
            Page newPage = new Page();
            PageId newPageId = SystemDefs.JavabaseBM.newPage(newPage,1);
            if (newPageId == null)
                throw new ConstructPageException(null, "new page failed");
            this.init(newPageId, newPage);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CBMPage(PageId pageId) throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageId, this, false);
            curPage = new PageId(Convert.getIntValue(CUR_PAGE_POS,data));
            slotsUsed = Convert.getShortValue(SLOTS_USED_POS,data);
        } catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }


    public boolean canInsert(byte bit) {

        boolean canInsert = (slotsUsed < 203);

        if (!canInsert) {
            canInsert = data[slotsUsed*SLOT_SIZE+1] == bit;
        }
        return canInsert;
    }


    public void dumpPage() throws IOException {
        curPage.pid =  Convert.getIntValue(CUR_PAGE_POS, data);
        slotsUsed =  Convert.getShortValue (SLOTS_USED_POS, data);

        System.out.println("dumpPage");
        System.out.println("curPage= " + curPage.pid);
        System.out.println("freeSpace= " + available_space());
        System.out.println("slotsUsed= " + slotsUsed);
    }


    public void init(PageId pageNo, Page page) throws IOException {
        data = page.getpage();

        slotsUsed = 0;
        Convert.setShortValue(slotsUsed, SLOTS_USED_POS, data);

        curPage.pid = pageNo.pid;
        Convert.setIntValue(curPage.pid, CUR_PAGE_POS, data);
    }


    private void setUsedSlots(short slotsUsed) throws IOException {
        Convert.setShortValue(slotsUsed, SLOTS_USED_POS, data);
    }

    public void insertBit(byte bit) throws IOException {

        if (slotsUsed == 0) {
            slotsUsed+=1;
            data[IP_FIXED_DATA] = bit;
            Convert.setIntValue(1,IP_FIXED_DATA+1,data);
            setUsedSlots(slotsUsed);
            return;
        }
        int checkPos = slotsUsed*SLOT_SIZE + IP_FIXED_DATA - 5;

        if (data[checkPos] == bit) {
            int x = Convert.getIntValue(checkPos+1,data);
            x += 1;
            Convert.setIntValue(x,checkPos+1,data);
        } else {
            int targetPos = checkPos+5;
            data[targetPos] = bit;
            Convert.setIntValue(1,targetPos+1,data);
            slotsUsed++;
            setUsedSlots(slotsUsed);
        }
    }

    public Integer getNextBit() {
        if (currentReadSlot >= slotsUsed) {
            return null;
        }

        try {
            int targetPosition = IP_FIXED_DATA +currentReadSlot*SLOT_SIZE;
            int currentSlotSize = Convert.getIntValue(targetPosition+1,data);

            if (slotCount <= currentSlotSize) {
                slotCount += 1;
                return (int) data[targetPosition];
            }

            currentReadSlot+=1;
            slotCount = 1;

            return getNextBit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public PageId getCurPage() {
        return this.curPage;
    }
}