package bitmap;

import btree.*;
import columnar.ColumnFile;
import columnar.ValueInt;
import columnar.ValueString;
import global.*;
import heap.*;

import java.io.IOException;

public class BitmapFile extends IndexFile implements GlobalConst {

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
        try {
            Scan columnScan = columnFile.getFile().openScan();
            Tuple tuple = new Tuple();
            RID rid = new RID();

            BMPage bmPage = new BMPage();

            while ((tuple = columnScan.getNext(rid)) != null) {

                if (bmPage.isSpaceAvailable()) {
                    
                    if (isValueGreaterThanEqualToTuple(value, tuple)) {
                        bmPage.insertBit((byte)1);
                    } else {
                        bmPage.insertBit((byte)0);
                    }
                } else {
                    headerFile.insertRecord(new BMDataPageInfo(bmPage.curPage,1, MAX_RECORD_COUNT).convertToTuple().getTupleByteArray());
                    unpinPage(bmPage.curPage, false);
                    bmPage = new BMPage();
                }
                
                headerFile.insertRecord(
                    new BMDataPageInfo(bmPage.curPage,
                    bmPage.isSpaceAvailable() ? 0 : 1 , 
                    MAX_RECORD_COUNT).convertToTuple().getTupleByteArray());
                
                    unpinPage(bmPage.curPage, false);
            }
            columnScan.closescan();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Integer getBit(PageId pageId, int indexPagePos) throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
	    BMPage page = new BMPage(pageId);
        return page.getBit(indexPagePos);
    }

    private boolean isValueGreaterThanEqualToTuple(ValueClass value, Tuple tuple) throws IOException, FieldNumberOutOfBoundException {
        if (value instanceof ValueInt) {
            return ((ValueInt) value).getValue() >= Convert.getIntValue(0, tuple.getTupleByteArray());
        } else if (value instanceof ValueString) {

            short size = Convert.getShortValue(0, tuple.getTupleByteArray());
            String record = Convert.getStrValue(0, tuple.getTupleByteArray(), size +2 );

            return ((ValueString) value).getValue().compareTo(record) >= 0;
        }
        return false;
    }

    public boolean insert(int position) throws IOException, PinPageException, UnpinPageException, ConstructPageException, InvalidTupleSizeException, FieldNumberOutOfBoundException {
		return set(position,1);	 
	}

    public boolean delete(int position) throws IOException, PinPageException, UnpinPageException, ConstructPageException, InvalidTupleSizeException, FieldNumberOutOfBoundException {
		return set(position,0);	 
	}

    public boolean set(int position, int bit) throws IOException, ConstructPageException, InvalidTupleSizeException, FieldNumberOutOfBoundException, PinPageException, UnpinPageException {

        int currentDataPage = 1;
        Tuple tuple;

        RID rid = new RID();
        Scan scan = headerFile.openScan();

        while (position > (currentDataPage*MAX_RECORD_COUNT)) {
            tuple = scan.getNext(rid);
        }

        tuple = scan.getNext(rid);
        BMDataPageInfo bmDataPageInfo = new BMDataPageInfo(tuple);

        BMPage bmPage = new BMPage(bmDataPageInfo.pageId);

        pinPage(bmPage.curPage);

        int pagePosition = position - (currentDataPage-1)*MAX_RECORD_COUNT;
        bmPage.setBit( pagePosition, (byte)bit);
        unpinPage(bmPage.curPage, false);
		return true;
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

    @Override
    public void insert(KeyClass data, RID rid) throws KeyTooLongException, KeyNotMatchException, LeafInsertRecException,
            IndexInsertRecException, ConstructPageException, UnpinPageException, PinPageException,
            NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException {
        throw new UnsupportedOperationException("Unimplemented method 'insert'");
    }

    @Override
    public boolean Delete(KeyClass data, RID rid)
            throws DeleteFashionException, LeafRedistributeException, RedistributeException, InsertRecException,
            KeyNotMatchException, UnpinPageException, IndexInsertRecException, FreePageException,
            RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
            ConstructPageException, DeleteRecException, IndexSearchException, IOException {
        throw new UnsupportedOperationException("Unimplemented method 'Delete'");
    }
}