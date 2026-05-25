package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.oracle;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;

public class OracleSqlParser {

    public static Map<String, Object> parseInsert(String sql) {
        Map<String, Object> data = new LinkedHashMap<>();
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (stmt instanceof Insert insert) {
                List<String> columns = insert.getColumns().stream()
                        .map(c -> c.getColumnName().replace("\"", ""))
                        .toList();

                ExpressionList<?> valuesList = (ExpressionList<?>) insert.getValues().getExpressions();

                for (int i = 0; i < columns.size(); i++) {
                    String cleanValue = valuesList.get(i).toString().replaceAll("^'|'$", "");
                    data.put(columns.get(i), cleanValue);
                }
            }
        } catch (Exception e) {
            data.put("raw_sql", sql);
        }
        return data;
    }

    public static Map<String, Object>[] parseUpdateWithDelta(String sql) {
        Map<String, Object> before = new LinkedHashMap<>();
        Map<String, Object> after = new LinkedHashMap<>();
        Map<String, Object> realAfterDelta = new LinkedHashMap<>();

        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (stmt instanceof Update update) {

                UpdateSet updateSet = update.getUpdateSets().get(0);
                List<String> setColumns = updateSet.getColumns().stream()
                        .map(c -> c.getColumnName().replace("\"", "").toUpperCase())
                        .toList();
                List<Expression> setExpressions = Collections.singletonList(updateSet.getValues());

                for (int i = 0; i < setColumns.size(); i++) {
                    String cleanValue = setExpressions.get(i).toString().replaceAll("^'|'$", "");
                    after.put(setColumns.get(i), cleanValue);
                }

                Expression where = update.getWhere();
                extractColumnsFromWhereClause(where, before);

                for (Map.Entry<String, Object> entry : after.entrySet()) {
                    String col = entry.getKey();
                    Object afterVal = entry.getValue();
                    Object beforeVal = before.get(col);

                    if (beforeVal == null || !beforeVal.equals(afterVal)) {
                        realAfterDelta.put(col, afterVal);
                    }
                }
            }
        } catch (Exception e) {
            realAfterDelta.put("raw_sql", sql);
        }

        return new Map[]{before, realAfterDelta};
    }

    private static void extractColumnsFromWhereClause(Expression expr, Map<String, Object> beforeMap) {
        if (expr instanceof AndExpression and) {
            extractColumnsFromWhereClause(and.getLeftExpression(), beforeMap);
            extractColumnsFromWhereClause(and.getRightExpression(), beforeMap);
        } else if (expr instanceof EqualsTo equals) {
            String colName = equals.getLeftExpression().toString().replace("\"", "").toUpperCase();
            String colValue = equals.getRightExpression().toString().replaceAll("^'|'$", "");
            beforeMap.put(colName, colValue);
        }
    }
}