package com.leonardoramos.rootl_cdcpublisher.adapters.inbound.oracle;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
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
 * Classe responsável por analisar e extrair informações de instruções SQL de INSERT, UPDATE e DELETE específicas para o Oracle.
 * Utiliza a biblioteca JSqlParser para interpretar as instruções SQL e extrair os dados relevantes, como os valores das colunas para INSERT e UPDATE, e as condições para DELETE.
 */
public class OracleSqlParser {

    /**
     * Analisa uma instrução SQL de INSERT e extrai os valores das colunas para criar um mapa de dados.
     * @param sql A instrução SQL de INSERT a ser analisada.
     * @return Um mapa contendo os nomes das colunas como chaves e os valores correspondentes como valores. Se a análise falhar, o mapa conterá a instrução SQL bruta sob a chave "raw_sql".
     */
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

    /**
     * Analisa uma instrução SQL de UPDATE e extrai os valores das colunas antes e depois da atualização, criando dois mapas de dados: um para os valores antes da atualização e outro para os valores depois da atualização (delta).
     * @param sql A instrução SQL de UPDATE a ser analisada.
     * @return Um array de mapas contendo dois elementos: o primeiro mapa representa os valores das colunas antes da atualização, e o segundo mapa representa os valores das colunas depois da atualização. Se a análise falhar, o segundo mapa conterá a instrução SQL bruta sob a chave "raw_sql".
     */
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

    /**
     * Analisa uma instrução SQL de DELETE e extrai as condições da cláusula WHERE para criar um mapa de dados representando os valores das colunas antes da exclusão. Se a análise falhar, o mapa conterá a instrução SQL bruta sob a chave "raw_sql".
     * @param sql A instrução SQL de DELETE a ser analisada.
     * @return Um mapa contendo os nomes das colunas como chaves e os valores correspondentes como valores, representando os dados antes da exclusão. Se a análise falhar, o mapa conterá a instrução SQL bruta sob a chave "raw_sql".
     */
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
     * Método auxiliar para extrair os nomes das colunas e seus valores da cláusula WHERE de uma instrução SQL. Suporta expressões AND e condições de igualdade (EqualsTo). Os nomes das colunas são convertidos para maiúsculas e os valores são limpos de aspas simples.
     * @param expr A expressão da cláusula WHERE a ser analisada.
     * @param beforeMap O mapa onde os nomes das colunas e seus valores extraídos serão armazenados. O mapa é modificado diretamente dentro do método.
     */
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