package commands;

import columnar.ColumnarFile;
import columnar.TupleScan;
import columnar.Utils;
import global.AttrType;
import global.SystemDefs;
import heap.Tuple;
import iterator.OutputTupleAttributes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileScan {

    private String[] targetColumnNames;
    private AttrType[] attrTypes;
    private TupleScan tupleScan;
    public FileScan(String columnarFileName, String[] targetColumnNames) {

        ColumnarFile columnarFile = new ColumnarFile(columnarFileName);

        this.targetColumnNames = (targetColumnNames.length != 0) ? targetColumnNames : columnarFile.getColumnNames();
        attrTypes = columnarFile.getAttrTypes();
//        tupleScan = new TupleScan(columnarFile);


    }

    void execute() {
        Tuple tuple;
        int count = 0;

        System.out.println(Utils.getHeaderString(targetColumnNames));

        while ((tuple = tupleScan.get_next()) != null) {
            System.out.println(Utils.getTupleString(tuple,attrTypes));
            count++;
        }

        System.out.println("Number of tuples printed: " + count);

        tupleScan.closeScan();
    }

    //COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
    public static void main(String[] command) {
        String[] command = "testdb test1 [A,B,C,D] {(A > 5) or (B < 6)} 5".split(" ");
        if (command.length != 2 && command.length != 3) {
            System.out.println(command.length);
            System.out.println("Error: Incorrect number of input values.");
            return;
        }

        // Extract values from the array
        String columnDBName = command[0];

        String dbpath = "/tmp/"+ columnDBName;
        SystemDefs sysdef = new SystemDefs( dbpath, 0, 300, "Clock" );


        FileScan fileScan = getFileScan(command, columnDBName);
//        fileScan.execute();
    }

    private static FileScan getFileScan(String[] command, String columnDBName) {
        String columnarFileName = command[1];

        String[] targetColumnNames = new String[0];

        if (command.length == 4) {
            targetColumnNames = command[2].substring(1,command[2].length()-1).split(",");
        }

        FileScan fileScan = new FileScan(columnarFileName, targetColumnNames);
        return fileScan;
    }

    public static void parseInput(String input) {
        // Define patterns for extracting database name, table name, column names, conditions, and integer value
        Pattern dbNamePattern = Pattern.compile("(\\w+)\\s+(\\w+)\\s+\\[(.*?)\\]\\s+\\{(.*?)\\}\\s+(\\d+)");
        Matcher matcher = dbNamePattern.matcher(input);

        if (matcher.find()) {
            // Extract database name, table name, column names, conditions, and integer value
            String dbName = matcher.group(1);
            String tableName = matcher.group(2);
            String columnNamesStr = matcher.group(3);
            String conditionsStr = matcher.group(4);
            int integerValue = Integer.parseInt(matcher.group(5));

            ColumnarFile columnarFile = new ColumnarFile(tableName);
            OutputTupleAttributes outputTupleAttributes = new OutputTupleAttributes(columnarFile, columnNamesStr,conditionsStr);



            // Print parsed information
            System.out.println("Database Name: " + dbName);
            System.out.println("Table Name: " + tableName);
            System.out.println("Column Names: " + columnList);
            System.out.println("Conditions: " + conditions);
            System.out.println("Integer Value: " + integerValue);
        } else {
            System.out.println("Invalid input format");
        }
    }
}
