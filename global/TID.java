package global;

import java.io.IOException;
import java.util.Arrays;

public class TID {

    int numRIDs;

    int position;
    RID[] rids;

    boolean isDeleted =  false;

    public TID(int numRIDs, int position) {
        this.numRIDs = numRIDs;
        this.position = position;
        this.rids = new RID[numRIDs];

        for(int  i = 0 ;i < numRIDs; i++) {
            rids[i] = new RID();
        }
    }

    public TID(byte[] data) {
        try {
            numRIDs = Convert.getIntValue(0,data);
            position = Convert.getIntValue(4,data);
            isDeleted = Convert.getIntValue(8,data) == 1;
            rids = new RID[numRIDs];

            int offset = 12;
            for(int i = 0; i< numRIDs;i++) {
                int slotNo = Convert.getIntValue(offset,data);
                int pageNo = Convert.getIntValue(offset+4,data);
                rids[i] = new RID(new PageId(pageNo),slotNo);

                offset+=8;
            }
        } catch (IOException e) {
            throw new RuntimeException("Error getting value from byte array",e);
        }
    }

    public int getNumRIDs() {
        return numRIDs;
    }

    public int getPosition() {
        return position;
    }

    public void setRid(int colNo,RID rid) {
        rids[colNo] = rid;
    }
    public RID getRid(int colNo) {
        return rids[colNo];
    }

    public void markDeleted() {
        isDeleted = true;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public byte[] getBytes() {
        byte[] data = new byte[8*numRIDs+12];

        try {
            Convert.setIntValue(numRIDs, 0,data);
            Convert.setIntValue(position, 4,data);
            Convert.setIntValue(isDeleted ? 1 : 0,8,data);

            int offset = 12;
            for(int i = 0; i<numRIDs;i++){
                rids[i].writeToByteArray(data,offset);
                offset +=8;
            }

        } catch (IOException e) {
            throw new RuntimeException("Error wrting byte value",e);
        }

        return data;
    }

    @Override
    public String toString() {
        return "TID{" +
                "numRIDs=" + numRIDs +
                ", position=" + position +
                ", rids=" + Arrays.toString(rids) +
                ", isDeleted=" + isDeleted +
                '}';
    }
}
