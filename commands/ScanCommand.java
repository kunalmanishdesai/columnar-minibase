package commands;

import columnar.ColumnarFile;
import global.AttrType;
import global.SystemDefs;
import iterator.OutputTupleAttributes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ScanCommand {

    protected String[] targetColumnNames;
    protected final ColumnarFile columnarFile;

    protected final AttrType[] attrTypes;

    protected final OutputTupleAttributes outputTupleAttributes;

    public ScanCommand(String input) {

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

            columnarFile = new ColumnarFile(columnarFileName);
            outputTupleAttributes = new OutputTupleAttributes(columnarFile,conditionsStr,columnNamesStr);

            targetColumnNames = columnNamesStr.split(",");
            targetColumnNames = (targetColumnNames.length != 0) ? targetColumnNames : columnarFile.getColumnNames();

            attrTypes = outputTupleAttributes.getOutputAttrs();
        } else {
            throw new IllegalArgumentException("Invalid input format");
        }
    }

    abstract void execute();
}
