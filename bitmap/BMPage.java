package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

import java.io.IOException;

public class BMPage extends HFPage {

  public static final int IP_FIXED_DATA = (2 * 2) + (3 * 4);
  private static final int SLOTS_USED_POS = 0;
  private static final int FREE_SPACE_POS = 2;
  private static final int PREV_PAGE_POS = 4;
  private static final int NEXT_PAGE_POS = 8;
  private static final int CUR_PAGE_POS = 12;

  /**
   * No. of slots used
   */
  private short slotsUsed;

  /**
   * No. of bytes free in the data byte array
   */
  // private short freeSpace;

  /**
   * Pointer to prev data page
   */
  private PageId prevPage = new PageId();

  /**
   * Pointer to the next page
   */
  private PageId nextPage = new PageId();

  /**
   * PageId of this page
   */
  protected PageId curPage = new PageId();

  public BMPage() throws ConstructPageException {
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

  public BMPage(Page page) {
    data = page.getpage();
  }

  public BMPage(PageId pageId) throws ConstructPageException {
    super();
    try {

      SystemDefs.JavabaseBM.pinPage(pageId, this, false);
      curPage = new PageId(Convert.getIntValue(CUR_PAGE_POS,data));
      slotsUsed = Convert.getShortValue(SLOTS_USED_POS,data);
    } catch (Exception e) {
      throw new ConstructPageException(e, "pinpage failed");
    }
  }

  public boolean isSpaceAvailable() throws IOException {
    return (Convert.getShortValue(FREE_SPACE_POS, data) -  slotsUsed) != 0 ;
  }

  public void dumpPage() throws IOException {
    prevPage.pid =  Convert.getIntValue(PREV_PAGE_POS, data);
		curPage.pid =  Convert.getIntValue(CUR_PAGE_POS, data);
		nextPage.pid =  Convert.getIntValue(NEXT_PAGE_POS, data);
		slotsUsed =  Convert.getShortValue (SLOTS_USED_POS, data);

		System.out.println("dumpPage");
		System.out.println("curPage= " + curPage.pid);
		System.out.println("nextPage= " + nextPage.pid);
		System.out.println("freeSpace= " + available_space());
		System.out.println("slotsUsed= " + slotsUsed);
  }

  public void init(PageId pageNo, Page page) throws IOException {
    data = page.getpage();

    slotsUsed = 0;
    Convert.setShortValue(slotsUsed, SLOTS_USED_POS, data);

    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE_POS, data);

    Convert.setIntValue(INVALID_PAGE, PREV_PAGE_POS, data);
    Convert.setIntValue(INVALID_PAGE, NEXT_PAGE_POS, data);
    
    short freeSpace = (short) (MAX_SPACE - IP_FIXED_DATA);
    Convert.setShortValue (freeSpace, FREE_SPACE_POS, data);
  }

  public void openBMpage(Page page) {
    data = page.getpage();
  }

  public PageId getCurPagePos() throws IOException {
    curPage.pid = Convert.getIntValue(CUR_PAGE_POS, data);
    return curPage;
  }

  public PageId getNextPagePos() throws IOException {
    nextPage.pid = Convert.getIntValue(NEXT_PAGE_POS, data);
    return nextPage;
  }

  public PageId getPrevPagePos() throws IOException {
    prevPage.pid = Convert.getIntValue(PREV_PAGE_POS, data);
    return prevPage;
  }

  public void setCurPage(PageId pageNo) throws IOException {
    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE_POS, data);
  }

  public void setNextPage(PageId pageNo) throws IOException {
    nextPage.pid = pageNo.pid;
    Convert.setIntValue(nextPage.pid, NEXT_PAGE_POS, data);
  }

  public void setPrevPage(PageId pageNo) throws IOException {
    prevPage.pid = pageNo.pid;
    Convert.setIntValue(prevPage.pid, PREV_PAGE_POS, data);
  }

  public byte[] getBMpageArray() {
    return data;
  }

  public void writeBMPageArray(byte[] pageData) {
    data = pageData;
  }

  public short getUsedSlots() throws IOException {
    return this.slotsUsed;
  }

  public short getUsedSlotData() throws IOException {
    return Convert.getShortValue(SLOTS_USED_POS, data);
  }

  public int getSlotDataByPos(int pos) throws IOException {
    return Convert.getIntValue(pos, data);
  }

  public void setUsedSlots(short slotsUsed) throws IOException {
		Convert.setShortValue(slotsUsed, SLOTS_USED_POS, data);
	}

  public void setBit(int position, byte bit) throws IOException {
    if(position > slotsUsed) {
      throw new RuntimeException("Slot unused, fragmentation");
    }
    int targetPos = position + IP_FIXED_DATA;
    Convert.setIntValue(bit, targetPos, data);
  }

  public Integer getBit(int position) throws IOException {
    if(position >= slotsUsed) {
      return null;
    }
    int targetPos = position + IP_FIXED_DATA;
    return (int) data[targetPos];
  }

  public void insertBit(byte bit) throws IOException {
    int targetPos = slotsUsed + IP_FIXED_DATA;
    data[targetPos] = bit;
    slotsUsed++;
    setUsedSlots(slotsUsed);
  }

}