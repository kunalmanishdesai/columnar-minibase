package iterator;

import columnar.ColumnarFile;
import global.AttrOperator;
import global.AttrType;

import java.util.ArrayList;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OutputTupleAttributes {

    private final ColumnarFile columnarFile;

    private final AttrType[] outputAttrs;
    private final FldSpec[] prodSpec;
    private final CondExpr[] condExprs;

    private final AttrType[] conditionAttrs;

    public OutputTupleAttributes(ColumnarFile columnarFile, String conditionsString, String targetColumns) {
        this.columnarFile = columnarFile;

        String[] targetColumnNames = targetColumns.split(",");

        if (targetColumnNames.length == 1 && Objects.equals(targetColumnNames[0], "")) {
            targetColumnNames = columnarFile.getColumnNames();
        }

        prodSpec = new FldSpec[targetColumnNames.length];
        outputAttrs = new AttrType[targetColumnNames.length];

        for (int i = 0; i < targetColumnNames.length; i++) {
            int columnNo = columnarFile.getColumnNo(targetColumnNames[i].trim());
            prodSpec[i] = new FldSpec(new RelSpec(RelSpec.outer), columnNo+1);
            outputAttrs[i] = columnarFile.getAttrTypes()[columnNo];
        }




        // Split conditionsString using "AND" or "OR" as delimiters (case insensitive)


        Pattern pattern = Pattern.compile("\\b(?i)(AND|OR)\\b");
        Matcher matcher = pattern.matcher(conditionsString);
        ArrayList<String> logicalOperators = new ArrayList<>();
        while (matcher.find()) {
            logicalOperators.add(matcher.group());
        }

        ArrayList<CondExpr> condExprList = new ArrayList<>();

        String[] conditions = pattern.split(conditionsString);

        conditionAttrs = new AttrType[conditions.length];
        // Parse each condition string
        condExprList.add(parseCondition(0,conditions[0]));


        // Parse subsequent conditions
        for (int i = 1; i < conditions.length; i++) {
            if (logicalOperators.get(i - 1).trim().equalsIgnoreCase("AND")) {
                // If the previous logical operator is "AND", parse the condition
                condExprList.add(parseCondition(i,conditions[i]));
            } else if (logicalOperators.get(i - 1).trim().equalsIgnoreCase("OR")) {
                // If the previous logical operator is "OR", link the current condition with the previous one
                condExprList.get(condExprList.size() - 1).next = parseCondition(i,conditions[i]);
            }
        }

        condExprs = condExprList.toArray(new CondExpr[condExprList.size()+1]);
        condExprs[condExprs.length-1] = null;
    }

    private CondExpr parseCondition(int i, String conditionString) {
        conditionString = conditionString.trim();

        // If the condition string is empty, return null or handle it according to your requirements
        if (conditionString.isEmpty()) {
            return null; // or handle it appropriately
        }
        Pattern conditionPattern = Pattern.compile("(\\w+)\\s*([><=]+)\\s*(\\w+)");
        Matcher matcher = conditionPattern.matcher(conditionString.trim());

        if (matcher.find()) {
            String column = matcher.group(1);
            String operator = matcher.group(2);
            String value = matcher.group(3);

            int columnNo = columnarFile.getColumnNo(column);
            AttrType attrType = columnarFile.getAttrTypes()[columnNo];
            conditionAttrs[i] = attrType;

            CondExpr condExpr = new CondExpr();
            condExpr.type1 = new AttrType(AttrType.attrSymbol);
            condExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), columnNo+1);
            condExpr.op = getOperator(operator);

            if (attrType.attrType == AttrType.attrInteger) {
                condExpr.type2 = new AttrType(AttrType.attrInteger);
                condExpr.operand2.integer = Integer.parseInt(value);
            } else if (attrType.attrType == AttrType.attrString) {
                condExpr.type2 = new AttrType(AttrType.attrString);
                condExpr.operand2.string = value;
            } else if (attrType.attrType == AttrType.attrReal) {
                condExpr.type2 = new AttrType(AttrType.attrReal);
                condExpr.operand2.real = Float.parseFloat(value);
            }
            return condExpr;
        } else {
            throw new IllegalArgumentException("Invalid condition: " + conditionString);
        }
    }

    private AttrOperator getOperator(String operator) {
        switch (operator) {
            case ">=":
                return new AttrOperator(AttrOperator.aopGE);
            case ">":
                return new AttrOperator(AttrOperator.aopGT);
            case "=":
                return new AttrOperator(AttrOperator.aopEQ);
            case "<=":
                return new AttrOperator(AttrOperator.aopLE);
            case "<":
                return new AttrOperator(AttrOperator.aopLT);
            case "!=":
                return new AttrOperator(AttrOperator.aopNE);
            default:
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
    }

    public CondExpr[] getCondExprs() {
        return condExprs;
    }

    public FldSpec[] getProdSpec() {
        return prodSpec;
    }

    @Override
    public String toString() {
        AttrType[] attrType = columnarFile.getAttrTypes();

        StringBuilder stringBuilder = new StringBuilder("Columns: ");

        // Append column specifications
        for (FldSpec fldSpec : prodSpec) {
            stringBuilder.append(fldSpec.offset-1).append("|");
        }
        stringBuilder.append("\nConditions: ");

        // Append condition expressions
        for (int i = 0; i < condExprs.length; i++) {
            CondExpr condExpr = condExprs[i];
            while (condExpr != null) {
                stringBuilder.append(condExpr.operand1.symbol.offset)
                        .append(" ")
                        .append(condExpr.op.toString())
                        .append(" ");
                if (attrType[condExpr.operand1.symbol.offset-1].attrType == AttrType.attrInteger) {
                    stringBuilder.append(condExpr.operand2.integer);
                } else if (attrType[condExpr.operand1.symbol.offset-1].attrType == AttrType.attrString) {
                    stringBuilder.append(condExpr.operand2.string);
                } else if (attrType[condExpr.operand1.symbol.offset-1].attrType == AttrType.attrReal) {
                    stringBuilder.append(condExpr.operand2.real);
                }
                if (condExpr.next != null) {
                    stringBuilder.append(" OR ");
                }
                condExpr = condExpr.next;
            }
            if (i < condExprs.length - 1) {
                stringBuilder.append(" AND ");
            }
        }

        return stringBuilder.toString();
    }

    public AttrType[] getConditionAttrs() {
        return conditionAttrs;
    }


    public AttrType[] getOutputAttrs() {
        return outputAttrs;
    }
}