package io.github.excalibase.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;

import java.util.List;

/**
 * Interface for query building services providing SQL generation abstraction.
 * This interface matches the actual QueryBuilderService method signatures for multi-database support.
 */
public interface IQueryBuilderService {

    List<String> parseOrderBy(String order, TableInfo tableInfo);

    String buildSelectClause(String select, TableInfo tableInfo);

    String buildInsertQuery(String tableName, List<String> columns, TableInfo tableInfo);

    String buildBulkInsertQuery(String tableName, List<String> columns, int recordCount, TableInfo tableInfo);

    String buildUpdateQuery(String tableName, List<String> updateColumns, List<String> whereConditions, TableInfo tableInfo);

    String buildDeleteQuery(String tableName, List<String> whereConditions);

    String buildUpsertQuery(String tableName, List<String> columns, List<String> primaryKeyColumns,
                          List<String> updateColumns, TableInfo tableInfo);

    String buildBulkUpsertQuery(String tableName, List<String> columns, List<String> primaryKeyColumns,
                               List<String> updateColumns, int recordCount, TableInfo tableInfo);

    String[] parseCompositeKey(String compositeKey, int expectedKeyCount);

    List<String> buildCompositeKeyConditions(List<ColumnInfo> primaryKeyColumns, TableInfo tableInfo);

    String encodeCursor(String value);

    String decodeCursor(String cursor);
}