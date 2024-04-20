package iterator;

import heap.Tuple;

public interface TupleScanInterface {

    Tuple get_next();

    void closeScan();
}
