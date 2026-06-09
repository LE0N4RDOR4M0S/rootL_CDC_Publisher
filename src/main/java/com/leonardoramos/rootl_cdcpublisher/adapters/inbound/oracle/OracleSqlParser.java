package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.oracle;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.Expression;

import java.util.*;

/**
 * Classe responsável por analisar e extrair informações de instruções SQL específicas para o Oracle LogMiner.
 */
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
                    data.put(columns.get(i), "NULL".equalsIgnoreCase(cleanValue) ? null : cleanValue);
                }
            }
        } catch (Exception e) {
            data.put("raw_sql", sql);
        }
        return data;
    }

    /**
     * Analisa uma instrução SQL de UPDATE e retorna o Estado Completo (Full State).
     * O 'before' contém a fotografia integral da linha.
     * O 'after' contém a fotografia integral da linha com as modificações aplicadas.
     */
    public static Map<String, Object>[] parseUpdateFullState(String sql) {
        Map<String, Object> before = new LinkedHashMap<>();
        Map<String, Object> after = new LinkedHashMap<>();

        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (stmt instanceof Update update) {

                // 1. Extrai o estado COMPLETO anterior da cláusula WHERE
                Expression where = update.getWhere();
                if (where != null) {
                    extractColumnsFromWhereClause(where, before);
                }

                // 2. O estado AFTER nasce como uma cópia idêntica do BEFORE (todas as colunas)
                after.putAll(before);

                // 3. Sobrescreve apenas os valores que foram modificados pelo SET
                for (UpdateSet updateSet : update.getUpdateSets()) {
                    List<String> setColumns = updateSet.getColumns().stream()
                            .map(c -> c.getColumnName().replace("\"", "").toUpperCase())
                            .toList();

                    Expression valueExpr = updateSet.getValues();

                    if (valueExpr instanceof ExpressionList<?> exprList) {
                        List<?> expressions = exprList.getExpressions();
                        for (int i = 0; i < setColumns.size(); i++) {
                            String cleanValue = expressions.get(i).toString().replaceAll("^'|'$", "");
                            after.put(setColumns.get(i), "NULL".equalsIgnoreCase(cleanValue) ? null : cleanValue);
                        }
                    } else {
                        String cleanValue = valueExpr.toString().replaceAll("^'|'$", "");
                        Object finalVal = "NULL".equalsIgnoreCase(cleanValue) ? null : cleanValue;
                        for (String col : setColumns) {
                            after.put(col, finalVal);
                        }
                    }
                }
            }
        } catch (Exception e) {
            after.put("raw_sql", sql);
        }

        return new Map[]{before, after};
    }

    public static Map<String, Object> parseDelete(String sql) {
        Map<String, Object> before = new LinkedHashMap<>();
        try {
            Statement stmt = CCJSqlParserUtil.parse(sql);
            if (stmt instanceof Delete delete) {
                Expression where = delete.getWhere();
                if (where != null) {
                    extractColumnsFromWhereClause(where, before);
                }
            }
        } catch (Exception e) {
            before.put("raw_sql", sql);
        }
        return before;
    }

    /**
     * Mapeia as colunas extraídas do LogMiner, suportando verificações de nulidade (IS NULL).
     */
    private static void extractColumnsFromWhereClause(Expression expr, Map<String, Object> beforeMap) {
        if (expr instanceof AndExpression and) {
            extractColumnsFromWhereClause(and.getLeftExpression(), beforeMap);
            extractColumnsFromWhereClause(and.getRightExpression(), beforeMap);
        } else if (expr instanceof EqualsTo equals) {
            String colName = equals.getLeftExpression().toString().replace("\"", "").toUpperCase();
            String colValue = equals.getRightExpression().toString().replaceAll("^'|'$", "");
            beforeMap.put(colName, "NULL".equalsIgnoreCase(colValue) ? null : colValue);
        } else if (expr instanceof IsNullExpression isNull) { // CORREÇÃO VITAL: Captura colunas vazias
            String colName = isNull.getLeftExpression().toString().replace("\"", "").toUpperCase();
            beforeMap.put(colName, null);
        }
    }
}