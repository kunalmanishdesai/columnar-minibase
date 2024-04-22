package bitmap;

import btree.*;
import columnar.ColumnFile;
import columnar.ValueInt;
import columnar.ValueString;
import global.*;
import heap.*;

import java.io.IOException;

public class BitmapFile implements GlobalConst {

    private static final int MAX_RECORD_COUNT = 1008;

    private BitmapType bitmapType;
    public final Heapfile headerFile;
	private final String filename;


	// Constructor for creating a new file, when it doesn't exists
	public BitmapFile(String fileName,BitmapType bitmapType)
            throws Exception {
        this.filename = fileName + bitmapType;
        this.headerFile = new Heapfile(filename);
        this.bitmapType = bitmapType;
        // this.filename = filename;

//        accessColumn(columnFile, value, new RID());
    }

    public void accessColumn(ColumnFile columnFile, ValueClass value, RID startRID) throws Exception {
            Scan columnScan = columnFile.getFile().openScan();

            Tuple tuple;

            BMPageInterface bmPage = getNewBMPage();

            while ((tuple = columnScan.getNext(startRID)) != null) {

                byte bit = 0;
                if (isValueGreaterThanEqualToTuple(value, tuple)) {
                   bit = 1;
                }

                if (bmPage.canInsert(bit)) {
                    bmPage.insertBit(bit);
                } else {
                    headerFile.insertRecord(new BMDataPageInfo(bmPage.getCurPage(),1, MAX_RECORD_COUNT).convertToTuple().getTupleByteArray());
                    unpinPage(bmPage.getCurPage(), true);
                    bmPage = getNewBMPage();
                    bmPage.insertBit(bit);
                }
            }

            headerFile.insertRecord(
                    new BMDataPageInfo(bmPage.getCurPage(), 1 , MAX_RECORD_COUNT).convertToTuple().getTupleByteArray());

            unpinPage(bmPage.getCurPage(), true);
            columnScan.closescan();
    }

    private boolean isValueGreaterThanEqualToTuple(ValueClass value, Tuple tuple) throws IOException, FieldNumberOutOfBoundException {
        Tuple xTuple = new Tuple(tuple.getTupleByteArray());

        if (value instanceof ValueInt) {
            return ((ValueInt) value).getValue() >= xTuple.getIntFld(1);
        } else if (value instanceof ValueString) {

            String record = xTuple.getStrFld(1);

            return ((ValueString) value).getValue().compareTo(record) >= 0;
        }
        return false;
    }

  
  /** Destroy entire Bitmap file.
   *@exception IOException  error from the lower layer
   *@exception FreePageException error when free a page
   */
  public void destroyBitmapFile() 
    throws IOException,
	   FreePageException,InvalidTupleSizeException, FieldNumberOutOfBoundException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, HFDiskMgrException {

        Tuple tuple;
        RID rid = new RID();
        Scan scan = headerFile.openScan();
        
        while ((tuple = scan.getNext(rid)) != null) {
            BMDataPageInfo dataPageInfo = new BMDataPageInfo(tuple);

            PageId pageId = dataPageInfo.pageId;
            freePage(pageId);
        }

        scan.closescan();
        headerFile.deleteFile();
    }
  
    private void unpinPage(PageId pageno, boolean isDirty) throws UnpinPageException{ 
      try {
        SystemDefs.JavabaseBM.unpinPage(pageno, isDirty);    
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new UnpinPageException(e,"");
      } 
    }
  
    private void freePage(PageId pageno) throws FreePageException {
      try {
	    SystemDefs.JavabaseBM.freePage(pageno);    
      }
      catch (Exception e) {
	    e.printStackTrace();
	    throw new FreePageException(e,"");
      } 
      
    }

    private BMPageInterface getNewBMPage() {
      switch (bitmapType) {
          case BITMAP -> {
              return new BMPage();
          }

          case CBITMAP -> {
              return new CBMPage();
          }

          default -> {
              throw new IllegalArgumentException("!!! Error !!!,type not found");
          }
      }
    }
}