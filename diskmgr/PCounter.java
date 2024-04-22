package diskmgr;

public class PCounter {
    public static int rcounter;
    public static int wcounter;

    public static long startTime;
    //initialize is called when the DB is first initialized
    public static void initialize() {
        rcounter =0;
        wcounter =0;
        startTime = System.nanoTime();
    }
    //readcounter is incremented
    public static void readIncrement() {
        rcounter++;
    }
    //write counter is incremented
    public static void writeIncrement() {
        wcounter++;
    }



    public static void print() {
        System.out.println("No. of pages read " +rcounter);
        System.out.println("No. of pages written " +wcounter);

        long executionTime = (System.nanoTime() - startTime) / 1000000;
        System.out.println("Time taken: " +executionTime + " milliseconds");
    }
}
