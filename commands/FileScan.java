package commands;

import columnar.ColumnarFile;
import columnar.TupleScan;
import columnar.Utils;
import global.AttrType;
import global.SystemDefs;
import heap.Scan;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.OutputTupleAttributes;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileScan {

    private final String[] targetColumnNames;
    private final AttrType[] attrTypes;
    private final TupleScan tupleScan;
    public FileScan(String columnarFileName, OutputTupleAttributes outputTupleAttributes, String[] targetColumnNames) {

        ColumnarFile columnarFile = new ColumnarFile(columnarFileName);

        this.targetColumnNames = (targetColumnNames.length != 0) ? targetColumnNames : columnarFile.getColumnNames();
        attrTypes = outputTupleAttributes.getOutputAttrs();
        tupleScan = new TupleScan(columnarFile,outputTupleAttributes);
    }

    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = tupleScan.get_next()) != null) {
            System.out.println(Utils.getTupleString(tuple,attrTypes));
            count++;
        }

        tupleScan.closeScan();
        System.out.println("Number of tuples printed: " + count);
    }

    //COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
    public static void main(String[] command) {

        String test = command[0];
        FileScan fileScan = parseInput(test);
        fileScan.execute();
    }

    public static FileScan parseInput(String input) {
        // Define patterns for extracting database name, table name, column names, conditions, and integer value
        Pattern dbNamePattern = Pattern.compile("(\\w+)\\s+(\\w+)\\s+\\[(.*?)\\]\\s+\\{(.*?)\\}\\s+(\\d+)");
        Matcher matcher = dbNamePattern.matcher(input);

        if (matcher.find()) {
            // Extract database name, table name, column names, conditions, and integer value
            String columnDBName = matcher.group(1);
            String columnarFileName = matcher.group(2);
            String columnNamesStr = matcher.group(3);
            String conditionsStr = matcher.group(4);
            int numBuf = Integer.parseInt(matcher.group(5));

            String dbpath = "/tmp/"+ columnDBName;
            SystemDefs sysdef = new SystemDefs( dbpath, 0, numBuf, "Clock" );

            ColumnarFile columnarFile = new ColumnarFile(columnarFileName);
            OutputTupleAttributes outputTupleAttributes = new OutputTupleAttributes(columnarFile,conditionsStr,columnNamesStr);

            return new FileScan(columnarFileName, outputTupleAttributes, columnNamesStr.split(","));

        } else {
            System.out.println("Invalid input format");
            System.exit(1);
        }
        return null;
    }
}
