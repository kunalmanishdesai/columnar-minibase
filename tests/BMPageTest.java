package tests;

import bitmap.BMPage;
import global.SystemDefs;

import java.io.IOException;

import static global.GlobalConst.NUMBUF;


public class BMPageTest extends TestDriver {

    public BMPageTest() {
        super("BitmapTest");
    }

    @Override
    protected boolean test1() {
        boolean pass = true;

        try {

            BMPage bmPage = new BMPage();

            bmPage.insertBit((byte)1);
            if (bmPage.getBit(0) != 1) {
                System.out.println("Failed in insertion");
                pass = false;
            }

            bmPage.setBit(0,(byte)0);

            if (bmPage.getBit(0) != 0) {
                System.out.println("Failed in setting");
                pass = false;
            }

            System.out.println("BMPAGE TEST " + (pass ? "Passed" : "Failed") );
        } catch (Exception e) {
            pass = false;
            e.printStackTrace();
            System.out.println("Some error tests failed");
        }

        return pass;
    }

    @Override
    protected boolean runAllTests() {
        boolean _passAll = OK;

        //Running test1() to test6()
        if (!test1()) {
            _passAll = FAIL;
        }
        if (!test2()) {
            _passAll = FAIL;
        }
        // Add calls to other test methods here

        return _passAll;
    }

    public boolean runTests () {


        System.out.print ("\n" + "Running " + testName() + " tests...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs( dbpath, NUMBUF+20, NUMBUF, "Clock" );
        }

        catch (Exception e) {
            Runtime.getRuntime().exit(1);
        }

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here.  If we need to port this
        // program to other platform, the remove_cmd have to be
        // modified accordingly.
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);

        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }

    @Override
    protected String testName() {
        return "BitmapFileTest";
    }

    public static void main(String[] args) {
        BMPageTest bitmapFileTest = new BMPageTest();
        bitmapFileTest.runTests();
    }
}
