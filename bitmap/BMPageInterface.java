package bitmap;

import global.PageId;

import java.io.IOException;

public interface BMPageInterface {

    public PageId getCurPage();


    public boolean canInsert(byte bit);

    public void insertBit(byte bit) throws IOException;

    public Integer getNextBit();
}
