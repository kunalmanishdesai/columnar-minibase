package bitmap;

import btree.ConstructPageException;
import diskmgr.Page;
import global.Convert;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

import java.io.IOException;

public class BMPage extends HFPage {

  public int readPageCounter = IP_FIXED_DATA;

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

  public BMPage() {
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

  public boolean canInsert() throws IOException {
    return (slotsUsed + IP_FIXED_DATA) < PAGE_SIZE ;
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

  public void setCurPage(PageId pageNo) throws IOException {
    curPage.pid = pageNo.pid;
    Convert.setIntValue(curPage.pid, CUR_PAGE_POS, data);
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

  public Integer getBit(int position) {
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

  public Integer getNextBit() {

    if (readPageCounter-IP_FIXED_DATA >= slotsUsed) {
      return null;
    }

    int bit = (int) data[readPageCounter];
    this.readPageCounter+= 1;
    return bit;
  }
}