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
    public final Heapfile headerFile;
	private final String filename;


	// Constructor for creating a new file, when it doesn't exists
	public BitmapFile(ColumnFile columnFile, ValueClass value,String fileName)
            throws Exception {
        this.filename = fileName;
        this.headerFile = new Heapfile(filename);
        // this.filename = filename;

        accessColumn(columnFile, value);
    }

    private void accessColumn(ColumnFile columnFile, ValueClass value) throws Exception {
            Scan columnScan = columnFile.getFile().openScan();
            Tuple tuple;
            RID rid = new RID();

            BMPage bmPage = new BMPage();

            while ((tuple = columnScan.getNext(rid)) != null) {

                if (bmPage.canInsert()) {
                    
                    if (isValueGreaterThanEqualToTuple(value, tuple)) {
                        bmPage.insertBit((byte)1);
                    } else {
                        bmPage.insertBit((byte)0);
                    }
                } else {
                    headerFile.insertRecord(new BMDataPageInfo(bmPage.curPage,1, MAX_RECORD_COUNT).convertToTuple().getTupleByteArray());
                    unpinPage(bmPage.curPage, true);
                    bmPage = new BMPage();
                }
                

            }

            headerFile.insertRecord(
                    new BMDataPageInfo(bmPage.curPage,
                            bmPage.canInsert() ? 0 : 1 ,
                            MAX_RECORD_COUNT).convertToTuple().getTupleByteArray());

            unpinPage(bmPage.curPage, true);
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
   *@exception IteratorException iterator error
   *@exception UnpinPageException error  when unpin a page
   *@exception FreePageException error when free a page
   *@exception DeleteFileEntryException failed when delete a file from DM
   *@exception ConstructPageException error in BT page constructor 
   *@exception PinPageException failed when pin a page
 * @throws InvalidTupleSizeException 
 * @throws FieldNumberOutOfBoundException 
 * @throws HFDiskMgrException 
 * @throws HFBufMgrException 
 * @throws FileAlreadyDeletedException 
 * @throws InvalidSlotNumberException 
   */
  public void destroyBitmapFile() 
    throws IOException, 
	   IteratorException, 
	   UnpinPageException,
	   FreePageException,   
	   DeleteFileEntryException, 
	   ConstructPageException,
	   PinPageException, InvalidTupleSizeException, FieldNumberOutOfBoundException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException, HFDiskMgrException {

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
    
    private BMPage pinPage(PageId pageno) throws PinPageException {
      try {
        BMPage page=new BMPage();
        SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
        return page;
      }
      catch (Exception e) {
	e.printStackTrace();
	throw new PinPageException(e,"");
      }
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
}