package io.github.excalibase.service;

import io.github.excalibase.constant.ColumnTypeConstant;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class RestApiService {

    private static final Logger log = LoggerFactory.getLogger(RestApiService.class);
    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSchemaService schemaService;
    private final QueryComplexityService complexityService;
    private final RelationshipBatchLoader batchLoader;
    private final ObjectMapper objectMapper;
    
    // Special parameters that are not database columns
    private static final Set<String> CONTROL_PARAMETERS = Set.of(
        "offset", "limit", "orderBy", "orderDirection", "select", "order", "expand",
        "first", "after", "last", "before", "join", "fields", "include", "batch",
        "query", "variables", "fragment", "alias", "groupBy", "having", "distinct",
        "aggregate", "transform", "validate", "explain", "format", "stream"
    );
    
    // Maximum allowed values for security
    private static final int MAX_LIMIT = 1000;
    private static final int MAX_OFFSET = 100000;
    
    public RestApiService(JdbcTemplate jdbcTemplate, DatabaseSchemaService schemaService, 
                         QueryComplexityService complexityService, RelationshipBatchLoader batchLoader) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaService = schemaService;
        this.complexityService = complexityService;
        this.batchLoader = batchLoader;
        this.objectMapper = new ObjectMapper();
    }

    public Map<String, Object> getRecords(String tableName, MultiValueMap<String, String> allParams, 
                                        int offset, int limit, String orderBy, 
                                        String orderDirection, String select, String expand) {
        
        // Security validations
        if (offset < 0 || offset > MAX_OFFSET) {
            throw new IllegalArgumentException("Offset must be between 0 and " + MAX_OFFSET);
        }
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("Limit must be between 1 and " + MAX_LIMIT);
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "SELECT");
        
        // Validate query complexity
        complexityService.validateQueryComplexity(tableName, allParams, limit, expand);

        // Filter out control parameters to get only filter parameters
        MultiValueMap<String, String> filters = new org.springframework.util.LinkedMultiValueMap<>();
        if (allParams != null) {
            for (Map.Entry<String, List<String>> entry : allParams.entrySet()) {
                String key = entry.getKey();
                if (!CONTROL_PARAMETERS.contains(key)) {
                    filters.put(key, entry.getValue());
                }
            }
        }

        // Build query
        StringBuilder query = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        // SELECT clause
        if (select != null && !select.trim().isEmpty()) {
            String[] selectedColumns = select.split(",");
            Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());
            
            for (String col : selectedColumns) {
                String trimmedCol = col.trim();
                if (!validColumns.contains(trimmedCol)) {
                    throw new IllegalArgumentException("Invalid column: " + trimmedCol);
                }
            }
            query.append("SELECT ").append(select);
        } else {
            query.append("SELECT *");
        }
        
        query.append(" FROM ").append(tableName);
        
        // WHERE clause
        if (!filters.isEmpty()) {
            query.append(" WHERE ");
            List<String> conditions = parseFilters(filters, params, tableInfo);
            query.append(String.join(" AND ", conditions));
        }
        
        // ORDER BY clause
        if (orderBy != null && !orderBy.trim().isEmpty()) {
            boolean columnExists = tableInfo.getColumns().stream()
                .anyMatch(col -> col.getName().equals(orderBy));
            if (!columnExists) {
                throw new IllegalArgumentException("Invalid column for ordering: " + orderBy);
            }
            
            String direction = orderDirection.equalsIgnoreCase("desc") ? "DESC" : "ASC";
            query.append(" ORDER BY ").append(orderBy).append(" ").append(direction);
        }
        
        // Support PostgREST-style ordering with "order" parameter
        String order = allParams != null ? allParams.getFirst("order") : null;
        if (order != null && !order.trim().isEmpty() && (orderBy == null || orderBy.trim().isEmpty())) {
            List<String> orderClauses = parseOrderBy(order, tableInfo);
            if (!orderClauses.isEmpty()) {
                query.append(" ORDER BY ").append(String.join(", ", orderClauses));
            }
        }
        
        // LIMIT and OFFSET
        query.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
        
        // Execute query
        List<Map<String, Object>> records = jdbcTemplate.queryForList(query.toString(), params.toArray());
        
        // Expand relationships if requested
        if (expand != null && !expand.trim().isEmpty()) {
            records = expandRelationships(records, tableInfo, expand);
        }
        
        // Get total count for pagination
        String countQuery = "SELECT COUNT(*) FROM " + tableName;
        List<Object> countParams = new ArrayList<>();
        
        if (!filters.isEmpty()) {
            countQuery += " WHERE ";
            List<String> conditions = parseFilters(filters, countParams, tableInfo);
            countQuery += String.join(" AND ", conditions);
        }
        
        Long totalCount = jdbcTemplate.queryForObject(countQuery, Long.class, countParams.toArray());
        
        Map<String, Object> result = new HashMap<>();
        result.put("data", records);
        result.put("pagination", Map.of(
            "offset", offset,
            "limit", limit,
            "total", totalCount != null ? totalCount : 0,
            "hasMore", offset + limit < (totalCount != null ? totalCount : 0)
        ));
        
        // Clear batch cache to prevent memory issues
        batchLoader.clearCache();
        
        return result;
    }

    /**
     * Parse filters in PostgREST format: column=operator.value
     * Also supports OR conditions: or=(age.gte.18,student.is.true)
     */
    private List<String> parseFilters(MultiValueMap<String, String> filters, List<Object> params, TableInfo tableInfo) {
        List<String> conditions = new ArrayList<>();
        
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());
        
        for (Map.Entry<String, List<String>> filterEntry : filters.entrySet()) {
            String key = filterEntry.getKey();
            List<String> values = filterEntry.getValue();
            
            if (key.equals("or")) {
                // Handle OR conditions: or=(age.gte.18,student.is.true)
                for (String value : values) {
                    String orCondition = parseOrCondition(value, params, validColumns, tableInfo);
                    if (orCondition != null) {
                        conditions.add("(" + orCondition + ")");
                    }
                }
            } else {
                // Handle regular conditions: age=gte.18 (can have multiple values for AND logic)
                for (String value : values) {
                    String condition = parseCondition(key, value, params, validColumns, tableInfo);
                    if (condition != null) {
                        conditions.add(condition);
                    }
                }
            }
        }
        
        return conditions;
    }
    
    /**
     * Parse OR condition: or=(age.gte.18,student.is.true)
     */
    private String parseOrCondition(String orValue, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        // Remove parentheses if present
        if (orValue.startsWith("(") && orValue.endsWith(")")) {
            orValue = orValue.substring(1, orValue.length() - 1);
        }
        
        String[] orConditions = orValue.split(",");
        List<String> parsedConditions = new ArrayList<>();
        
        for (String condition : orConditions) {
            condition = condition.trim();
            // Split by first dot to get column and operator.value
            int firstDot = condition.indexOf('.');
            if (firstDot > 0) {
                String column = condition.substring(0, firstDot);
                String operatorValue = condition.substring(firstDot + 1);
                
                String parsedCondition = parseCondition(column, operatorValue, params, validColumns, tableInfo);
                if (parsedCondition != null) {
                    parsedConditions.add(parsedCondition);
                }
            }
        }
        
        return parsedConditions.isEmpty() ? null : String.join(" OR ", parsedConditions);
    }
    
    /**
     * Parse single condition: column=operator.value
     */
    private String parseCondition(String column, String value, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        // Validate column exists
        if (!validColumns.contains(column)) {
            throw new IllegalArgumentException("Invalid column for filtering: " + column);
        }
        
        // Basic SQL injection protection - check for common dangerous patterns
        String upperValue = value.toUpperCase();
        if (upperValue.contains(";") || upperValue.contains("--") || upperValue.contains("/*") || 
            upperValue.contains("*/") || upperValue.contains(" DROP ") || upperValue.contains(" DELETE ") ||
            upperValue.contains(" UPDATE ") || upperValue.contains(" INSERT ") || upperValue.contains(" CREATE ") ||
            upperValue.contains(" ALTER ") || upperValue.contains(" TRUNCATE ") || upperValue.contains("UNION ") ||
            upperValue.contains("EXEC") || upperValue.contains("EXECUTE")) {
            throw new IllegalArgumentException("Invalid characters detected in filter value");
        }
        
        // Parse operator.value format
        if (!value.contains(".")) {
            // No operator specified, default to equality
            params.add(convertValueToColumnType(column, value, tableInfo));
            return column + " = ?";
        }
        
        int firstDot = value.indexOf('.');
        String operator = value.substring(0, firstDot);
        String operatorValue = value.substring(firstDot + 1);
        
        switch (operator.toLowerCase()) {
            case "eq":
                params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " = ?";
                
            case "neq":
                params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " <> ?";
                
            case "gt":
                params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " > ?";
                
            case "gte":
                params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " >= ?";
                
            case "lt":
                params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " < ?";
                
            case "lte":
                params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " <= ?";
                
            case "like":
                params.add("%" + operatorValue + "%");
                return column + " LIKE ?";
                
            case "ilike":
                params.add("%" + operatorValue + "%");
                return column + " ILIKE ?";
                
            case "in":
                // Parse in.(value1,value2,value3) - only process content within parentheses for security
                if (operatorValue.startsWith("(") && operatorValue.contains(")")) {
                    // Find the closing parenthesis and only process content within parentheses
                    int closingParen = operatorValue.indexOf(')');
                    String inValues = operatorValue.substring(1, closingParen);
                    
                    // Basic validation - reject if contains suspicious characters
                    if (inValues.contains(";") || inValues.contains("--") || inValues.contains("/*") || 
                        inValues.contains("*/") || inValues.contains("DROP") || inValues.contains("DELETE") ||
                        inValues.contains("UPDATE") || inValues.contains("INSERT") || inValues.contains("CREATE")) {
                        throw new IllegalArgumentException("Invalid characters detected in filter value");
                    }
                    
                    String[] values = inValues.split(",");
                    List<String> placeholders = new ArrayList<>();
                    for (String val : values) {
                        String trimmedVal = val.trim();
                        if (!trimmedVal.isEmpty()) {
                            params.add(convertValueToColumnType(column, trimmedVal, tableInfo));
                            placeholders.add("?");
                        }
                    }
                    return column + " IN (" + String.join(",", placeholders) + ")";
                }
                break;
                
            case "is":
                // Handle is.null, is.true, is.false
                switch (operatorValue.toLowerCase()) {
                    case "null":
                        return column + " IS NULL";
                    case "true":
                        params.add(true);
                        return column + " = ?";
                    case "false":
                        params.add(false);
                        return column + " = ?";
                    default:
                        params.add(convertValueToColumnType(column, operatorValue, tableInfo));
                        return column + " = ?";
                }
                
            // Enhanced PostgreSQL type operations
            case "haskey":
                // JSON: column ? 'key'
                params.add(operatorValue);
                return column + " ?";
                
            case "haskeys":
                // JSON: column ?& array['key1','key2'] - ALL keys must exist
                if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
                    String keysStr = operatorValue.substring(1, operatorValue.length() - 1);
                    String[] keys = keysStr.split(",");
                    List<String> cleanKeys = new ArrayList<>();
                    for (String key : keys) {
                        cleanKeys.add(key.trim().replace("\"", ""));
                    }
                    // Use PostgreSQL array literal syntax
                    String arrayLiteral = "ARRAY[" + cleanKeys.stream()
                        .map(key -> "'" + key + "'")
                        .collect(Collectors.joining(",")) + "]";
                    return column + " ?& " + arrayLiteral;
                } else {
                    throw new IllegalArgumentException("haskeys operator requires array format: [\"key1\",\"key2\"]");
                }
                
            case "hasanykeys":
                // JSON: column ?| array['key1','key2'] - ANY key must exist
                if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
                    String keysStr = operatorValue.substring(1, operatorValue.length() - 1);
                    String[] keys = keysStr.split(",");
                    List<String> cleanKeys = new ArrayList<>();
                    for (String key : keys) {
                        cleanKeys.add(key.trim().replace("\"", ""));
                    }
                    // Use PostgreSQL array literal syntax
                    String arrayLiteral = "ARRAY[" + cleanKeys.stream()
                        .map(key -> "'" + key + "'")
                        .collect(Collectors.joining(",")) + "]";
                    return column + " ?| " + arrayLiteral;
                } else {
                    throw new IllegalArgumentException("hasanykeys operator requires array format: [\"key1\",\"key2\"]");
                }
                
            case "jsoncontains":
            case "contains":
                // JSON: column @> ? - Does left JSON value contain right JSON path/value entries at top level?
                try {
                    // Validate JSON format
                    objectMapper.readTree(operatorValue);
                    params.add(operatorValue);
                    return column + " @> ?::jsonb";
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid JSON format for contains operator: " + operatorValue);
                }
                
            case "jsoncontained":
            case "containedin":
                // JSON: column <@ ? - Are left JSON path/value entries contained at top level within right JSON value?
                try {
                    // Validate JSON format
                    objectMapper.readTree(operatorValue);
                    params.add(operatorValue);
                    return column + " <@ ?::jsonb";
                } catch (Exception e) {
                    throw new IllegalArgumentException("Invalid JSON format for containedin operator: " + operatorValue);
                }
                
            case "jsonexists":
            case "exists":
                // JSON: column ? ? - Does the string exist as a top-level key within the JSON value?
                params.add(operatorValue);
                return column + " ? ?";
                
            case "jsonexistsany":
            case "existsany":
                // JSON: column ?| ? - Do any of these array strings exist as top-level keys?
                if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
                    String keysStr = operatorValue.substring(1, operatorValue.length() - 1);
                    String[] keys = keysStr.split(",");
                    List<String> cleanKeys = new ArrayList<>();
                    for (String key : keys) {
                        cleanKeys.add(key.trim().replace("\"", ""));
                    }
                    String arrayLiteral = "ARRAY[" + cleanKeys.stream()
                        .map(key -> "'" + key + "'")
                        .collect(Collectors.joining(",")) + "]";
                    return column + " ?| " + arrayLiteral;
                } else {
                    throw new IllegalArgumentException("existsany operator requires array format: [\"key1\",\"key2\"]");
                }
                
            case "jsonexistsall":
            case "existsall":
                // JSON: column ?& ? - Do all of these array strings exist as top-level keys?
                if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
                    String keysStr = operatorValue.substring(1, operatorValue.length() - 1);
                    String[] keys = keysStr.split(",");
                    List<String> cleanKeys = new ArrayList<>();
                    for (String key : keys) {
                        cleanKeys.add(key.trim().replace("\"", ""));
                    }
                    String arrayLiteral = "ARRAY[" + cleanKeys.stream()
                        .map(key -> "'" + key + "'")
                        .collect(Collectors.joining(",")) + "]";
                    return column + " ?& " + arrayLiteral;
                } else {
                    throw new IllegalArgumentException("existsall operator requires array format: [\"key1\",\"key2\"]");
                }
                
            case "jsonpath":
                // JSON path query: column @? ? - Does JSON path return any item for the specified JSON value?
                params.add(operatorValue);
                return column + " @? ?::jsonpath";
                
            case "jsonpathexists":
                // JSON path exists: column @@ ? - Returns the result of JSON path predicate check for the specified JSON value
                params.add(operatorValue);
                return column + " @@ ?::jsonpath";
                
            case "arraycontains":
                // Array: column @> ARRAY[?]
                params.add(operatorValue);
                return column + " @> ARRAY[?]";
                
            case "arrayhasany":
                // Array: column && ARRAY[values]
                if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
                    String valuesStr = operatorValue.substring(1, operatorValue.length() - 1);
                    String[] values = valuesStr.split(",");
                    List<String> cleanValues = new ArrayList<>();
                    for (String val : values) {
                        cleanValues.add(val.trim().replace("\"", ""));
                    }
                    params.add(cleanValues.toArray(new String[0]));
                    return column + " && ?";
                }
                break;
                
            case "arrayhasall":
                // Array: column @> ARRAY[values]
                if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
                    String valuesStr = operatorValue.substring(1, operatorValue.length() - 1);
                    String[] values = valuesStr.split(",");
                    List<String> cleanValues = new ArrayList<>();
                    for (String val : values) {
                        cleanValues.add(val.trim().replace("\"", ""));
                    }
                    params.add(cleanValues.toArray(new String[0]));
                    return column + " @> ?";
                }
                break;
                
            case "arraylength":
                // Array length: array_length(column, 1) = ?
                params.add(Integer.parseInt(operatorValue));
                return "array_length(" + column + ", 1) = ?";
                
            case "startswith":
                // String starts with
                params.add(operatorValue + "%");
                return column + " LIKE ?";
                
            case "endswith":
                // String ends with
                params.add("%" + operatorValue);
                return column + " LIKE ?";
                
            case "notin":
                // NOT IN operation - only process content within parentheses for security
                if (operatorValue.startsWith("(") && operatorValue.contains(")")) {
                    // Find the closing parenthesis and only process content within parentheses
                    int closingParen = operatorValue.indexOf(')');
                    String inValues = operatorValue.substring(1, closingParen);
                    
                    // Basic validation - reject if contains suspicious characters
                    if (inValues.contains(";") || inValues.contains("--") || inValues.contains("/*") || 
                        inValues.contains("*/") || inValues.contains("DROP") || inValues.contains("DELETE") ||
                        inValues.contains("UPDATE") || inValues.contains("INSERT") || inValues.contains("CREATE")) {
                        throw new IllegalArgumentException("Invalid characters detected in filter value");
                    }
                    
                    String[] values = inValues.split(",");
                    List<String> placeholders = new ArrayList<>();
                    for (String val : values) {
                        String trimmedVal = val.trim();
                        if (!trimmedVal.isEmpty()) {
                            params.add(convertValueToColumnType(column, trimmedVal, tableInfo));
                            placeholders.add("?");
                        }
                    }
                    return column + " NOT IN (" + String.join(",", placeholders) + ")";
                }
                break;
                
            case "isnotnull":
                // IS NOT NULL
                return column + " IS NOT NULL";
                
            default:
                // Unknown operator, treat as equality
                params.add(convertValueToColumnType(column, value, tableInfo));
                return column + " = ?";
        }
        
        // Fallback to equality
        params.add(convertValueToColumnType(column, value, tableInfo));
        return column + " = ?";
    }
    
    /**
     * Convert string value to appropriate type based on column metadata from table schema
     */
    private Object convertValueToColumnType(String columnName, String value, TableInfo tableInfo) {
        try {
            // Find the column in the table schema
            ColumnInfo column = tableInfo.getColumns().stream()
                .filter(col -> col.getName().equals(columnName))
                .findFirst()
                .orElse(null);
            
            if (column == null) {
                // Column not found, use basic pattern matching
                return convertValueToColumnType(columnName, value);
            }
            
            String columnType = column.getType().toLowerCase();
            
            // Handle PostgreSQL enhanced types
            if (columnType.startsWith(ColumnTypeConstant.POSTGRES_ENUM + ":")) {
                // PostgreSQL enum type - validate against enum values
                String enumTypeName = columnType.substring((ColumnTypeConstant.POSTGRES_ENUM + ":").length());
                return validateEnumValue(enumTypeName, value);
            } else if (columnType.startsWith(ColumnTypeConstant.POSTGRES_COMPOSITE + ":")) {
                // PostgreSQL composite type - keep as string for now (could be enhanced to parse JSON)
                return value;
            } else if (columnType.equals(ColumnTypeConstant.INET) || columnType.equals(ColumnTypeConstant.CIDR)) {
                // Network types - validate format
                return validateNetworkAddress(value);
            } else if (columnType.equals(ColumnTypeConstant.MACADDR) || columnType.equals(ColumnTypeConstant.MACADDR8)) {
                // MAC address types - validate format
                return validateMacAddress(value);
            } else if (columnType.contains("int") || columnType.equals("serial") || 
                columnType.equals("bigserial") || columnType.equals("smallserial")) {
                // Integer types
                return Integer.parseInt(value);
            } else if (columnType.contains("decimal") || columnType.contains("numeric") || 
                       columnType.contains("real") || columnType.contains("double") || 
                       columnType.equals("float")) {
                // Decimal types
                return Double.parseDouble(value);
            } else if (columnType.equals("boolean") || columnType.equals("bool")) {
                // Boolean types
                return Boolean.parseBoolean(value);
            } else if (columnType.contains("timestamp") || columnType.contains("date") || 
                       columnType.contains("time")) {
                // Date/time types - keep as string for SQL parsing
                return value;
            } else {
                // Default to string for varchar, text, json, etc.
                return value;
            }
        } catch (NumberFormatException e) {
            // If conversion fails, return as string
            return value;
        }
    }
    
    /**
     * Convert string value to appropriate type based on basic pattern matching
     */
    private Object convertValueToColumnType(String columnName, String value) {
        try {
            // Try to convert to number if it looks like a number
            if (value.matches("-?\\d+")) {
                // Integer
                return Integer.parseInt(value);
            } else if (value.matches("-?\\d+\\.\\d+")) {
                // Decimal
                return Double.parseDouble(value);
            } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                // Boolean
                return Boolean.parseBoolean(value);
            } else {
                // String - keep as is
                return value;
            }
        } catch (NumberFormatException e) {
            // If conversion fails, return as string
            return value;
        }
    }
    
    /**
     * Parse PostgREST-style order parameter: order=age.desc,height.asc
     */
    private List<String> parseOrderBy(String order, TableInfo tableInfo) {
        List<String> orderClauses = new ArrayList<>();
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());
        
        String[] orderParts = order.split(",");
        for (String part : orderParts) {
            part = part.trim();
            
            String column;
            String direction = "ASC";
            String nullsOrder = "";
            
            if (part.contains(".")) {
                String[] dotParts = part.split("\\.");
                column = dotParts[0];
                
                for (int i = 1; i < dotParts.length; i++) {
                    String modifier = dotParts[i].toLowerCase();
                    if (modifier.equals("asc") || modifier.equals("desc")) {
                        direction = modifier.toUpperCase();
                    } else if (modifier.equals("nullsfirst")) {
                        nullsOrder = " NULLS FIRST";
                    } else if (modifier.equals("nullslast")) {
                        nullsOrder = " NULLS LAST";
                    }
                }
            } else {
                column = part;
            }
            
            if (!validColumns.contains(column)) {
                throw new IllegalArgumentException("Invalid column for ordering: " + column);
            }
            
            orderClauses.add(column + " " + direction + nullsOrder);
        }
        
        return orderClauses;
    }

    public Map<String, Object> getRecord(String tableName, String id, String select, String expand) {
        // Security validations
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("ID cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "SELECT");

        // Find primary key column
        String primaryKeyColumn = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id"); // fallback to 'id'

        // Build query
        String selectClause = "*";
        if (select != null && !select.trim().isEmpty()) {
            String[] selectedColumns = select.split(",");
            Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());
            
            for (String col : selectedColumns) {
                String trimmedCol = col.trim();
                if (!validColumns.contains(trimmedCol)) {
                    throw new IllegalArgumentException("Invalid column: " + trimmedCol);
                }
            }
            selectClause = select;
        }

        String query = "SELECT " + selectClause + " FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?";
        
        // Convert ID to appropriate type based on primary key column type
        Object convertedId = convertValueToColumnType(primaryKeyColumn, id);
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query, convertedId);
        if (results.isEmpty()) {
            return null;
        }
        
        Map<String, Object> result = results.get(0);
        
        // Expand relationships if requested
        if (expand != null && !expand.trim().isEmpty()) {
            List<Map<String, Object>> singleRecord = List.of(result);
            List<Map<String, Object>> expandedRecords = expandRelationships(singleRecord, tableInfo, expand);
            result = expandedRecords.isEmpty() ? result : expandedRecords.get(0);
        }
        
        // Clear batch cache to prevent memory issues
        batchLoader.clearCache();
        
        return result;
    }

    public Map<String, Object> getRecordsWithCursor(String tableName, MultiValueMap<String, String> allParams, 
                                                   String first, String after, String last, String before,
                                                   String orderBy, String orderDirection, String select, String expand) {
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "SELECT");
        
        // Determine pagination parameters
        int limit = 100; // Default
        boolean forward = true; // Default direction
        
        if (first != null) {
            limit = Math.min(Integer.parseInt(first), MAX_LIMIT);
            forward = true;
        } else if (last != null) {
            limit = Math.min(Integer.parseInt(last), MAX_LIMIT);
            forward = false;
        }
        
        // Validate query complexity
        complexityService.validateQueryComplexity(tableName, allParams, limit, expand);
        
        // Get primary key for cursor
        String primaryKeyColumn = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id");
        
        // If no explicit orderBy, use primary key
        if (orderBy == null || orderBy.trim().isEmpty()) {
            orderBy = primaryKeyColumn;
        }
        
        // Filter out control parameters to get only filter parameters
        MultiValueMap<String, String> filters = new org.springframework.util.LinkedMultiValueMap<>();
        if (allParams != null) {
            for (Map.Entry<String, List<String>> entry : allParams.entrySet()) {
                String key = entry.getKey();
                if (!CONTROL_PARAMETERS.contains(key)) {
                    filters.put(key, entry.getValue());
                }
            }
        }
        
        // Build query
        StringBuilder query = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        // SELECT clause
        if (select != null && !select.trim().isEmpty()) {
            String[] selectedColumns = select.split(",");
            Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());
            
            for (String col : selectedColumns) {
                String trimmedCol = col.trim();
                if (!validColumns.contains(trimmedCol)) {
                    throw new IllegalArgumentException("Invalid column: " + trimmedCol);
                }
            }
            query.append("SELECT ").append(select);
        } else {
            query.append("SELECT *");
        }
        
        query.append(" FROM ").append(tableName);
        
        // WHERE clause (including cursor conditions)
        List<String> conditions = new ArrayList<>();
        
        // Add regular filters
        if (!filters.isEmpty()) {
            conditions.addAll(parseFilters(filters, params, tableInfo));
        }
        
        // Add cursor conditions
        if (after != null) {
            String decodedCursor = decodeCursor(after);
            if (forward) {
                conditions.add(orderBy + " > ?");
            } else {
                conditions.add(orderBy + " < ?");
            }
            params.add(convertValueToColumnType(orderBy, decodedCursor, tableInfo));
        }
        
        if (before != null) {
            String decodedCursor = decodeCursor(before);
            if (forward) {
                conditions.add(orderBy + " < ?");
            } else {
                conditions.add(orderBy + " > ?");
            }
            params.add(convertValueToColumnType(orderBy, decodedCursor, tableInfo));
        }
        
        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        
        // ORDER BY clause
        String direction = forward ? 
            (orderDirection.equalsIgnoreCase("desc") ? "DESC" : "ASC") :
            (orderDirection.equalsIgnoreCase("desc") ? "ASC" : "DESC"); // Reverse for backward pagination
            
        query.append(" ORDER BY ").append(orderBy).append(" ").append(direction);
        
        // LIMIT
        query.append(" LIMIT ").append(limit + 1); // Fetch one extra to check hasNextPage/hasPreviousPage
        
        // Execute query
        List<Map<String, Object>> records = jdbcTemplate.queryForList(query.toString(), params.toArray());
        
        // Check if there are more records
        boolean hasMore = records.size() > limit;
        if (hasMore) {
            records = records.subList(0, limit); // Remove the extra record
        }
        
        // If backward pagination, reverse the results
        if (!forward) {
            Collections.reverse(records);
        }
        
        // Expand relationships if requested
        if (expand != null && !expand.trim().isEmpty()) {
            records = expandRelationships(records, tableInfo, expand);
        }
        
        // Create edges with cursors
        List<Map<String, Object>> edges = new ArrayList<>();
        for (Map<String, Object> record : records) {
            Object cursorValue = record.get(orderBy);
            String cursor = encodeCursor(cursorValue != null ? cursorValue.toString() : "");
            
            Map<String, Object> edge = new HashMap<>();
            edge.put("node", record);
            edge.put("cursor", cursor);
            edges.add(edge);
        }
        
        // Create page info
        Map<String, Object> pageInfo = new HashMap<>();
        if (!edges.isEmpty()) {
            pageInfo.put("startCursor", edges.get(0).get("cursor"));
            pageInfo.put("endCursor", edges.get(edges.size() - 1).get("cursor"));
        } else {
            pageInfo.put("startCursor", null);
            pageInfo.put("endCursor", null);
        }
        
        if (forward) {
            pageInfo.put("hasNextPage", hasMore);
            pageInfo.put("hasPreviousPage", after != null);
        } else {
            pageInfo.put("hasNextPage", before != null);
            pageInfo.put("hasPreviousPage", hasMore);
        }
        
        // Get total count
        String countQuery = "SELECT COUNT(*) FROM " + tableName;
        List<Object> countParams = new ArrayList<>();
        if (!filters.isEmpty()) {
            countQuery += " WHERE ";
            List<String> countConditions = parseFilters(filters, countParams, tableInfo);
            countQuery += String.join(" AND ", countConditions);
        }
        Long totalCount = jdbcTemplate.queryForObject(countQuery, Long.class, countParams.toArray());
        
        // Build connection response
        Map<String, Object> result = new HashMap<>();
        result.put("edges", edges);
        result.put("pageInfo", pageInfo);
        result.put("totalCount", totalCount != null ? totalCount : 0);
        
        // Clear batch cache to prevent memory issues
        batchLoader.clearCache();
        
        return result;
    }
    
    /**
     * Encode cursor value to base64
     */
    private String encodeCursor(String value) {
        try {
            return java.util.Base64.getEncoder().encodeToString(value.getBytes("UTF-8"));
        } catch (Exception e) {
            return "";
        }
    }
    
    /**
     * Decode cursor value from base64
     */
    private String decodeCursor(String cursor) {
        try {
            return new String(java.util.Base64.getDecoder().decode(cursor), "UTF-8");
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid cursor: " + cursor);
        }
    }

    public Map<String, Object> createRecord(String tableName, Map<String, Object> data) {
        // Validate input
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "INSERT");

        // Validate columns
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());

        for (String column : data.keySet()) {
            if (!validColumns.contains(column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }

        // Build INSERT query
        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> values = new ArrayList<>(data.values());
        
        String columnList = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        
        // Try with RETURNING first, fall back to separate queries if not supported
        try {
            String query = "INSERT INTO " + tableName + " (" + columnList + ") VALUES (" + placeholders + ") RETURNING *";
            List<Map<String, Object>> results = jdbcTemplate.queryForList(query, values.toArray());
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.warn("RETURNING clause not supported, using fallback approach: " + e.getMessage());
            
            // Fallback: Insert without RETURNING, then select the record
            String insertQuery = "INSERT INTO " + tableName + " (" + columnList + ") VALUES (" + placeholders + ")";
            int rowsAffected = jdbcTemplate.update(insertQuery, values.toArray());
            
            if (rowsAffected > 0) {
                // Try to find the inserted record by matching the data
                // This is a simple approach - in production you'd want to use sequence values or generated keys
                StringBuilder selectQuery = new StringBuilder("SELECT * FROM " + tableName + " WHERE ");
                List<String> whereConditions = new ArrayList<>();
                List<Object> whereValues = new ArrayList<>();
                
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    whereConditions.add(entry.getKey() + " = ?");
                    whereValues.add(entry.getValue());
                }
                
                selectQuery.append(String.join(" AND ", whereConditions));
                selectQuery.append(" ORDER BY ");
                
                // Order by primary key if available
                String primaryKey = tableInfo.getColumns().stream()
                    .filter(ColumnInfo::isPrimaryKey)
                    .map(ColumnInfo::getName)
                    .findFirst()
                    .orElse(columns.get(0));
                
                selectQuery.append(primaryKey).append(" DESC LIMIT 1");
                
                List<Map<String, Object>> results = jdbcTemplate.queryForList(selectQuery.toString(), whereValues.toArray());
                return results.isEmpty() ? null : results.get(0);
            }
            
            return null;
        }
    }

    public List<Map<String, Object>> createBulkRecords(String tableName, List<Map<String, Object>> dataList) {
        // Validate input
        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("Data list cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "INSERT");
        
        // Validate each record and filter non-null values
        List<Map<String, Object>> processedRecords = new ArrayList<>();
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());
        
        for (Map<String, Object> data : dataList) {
            Map<String, Object> processedData = new HashMap<>();
            
            // Validate columns
            for (String column : data.keySet()) {
                if (!validColumns.contains(column)) {
                    throw new IllegalArgumentException("Invalid column: " + column);
                }
                if (data.get(column) != null) {
                    processedData.put(column, data.get(column));
                }
            }
            
            if (!processedData.isEmpty()) {
                processedRecords.add(processedData);
            }
        }
        
        if (processedRecords.isEmpty()) {
            throw new IllegalArgumentException("No valid records to create");
        }
        
        // Get all unique columns across all records
        Set<String> allColumns = processedRecords.stream()
            .flatMap(record -> record.keySet().stream())
            .collect(Collectors.toSet());
        
        // Build bulk INSERT query
        List<String> columnList = new ArrayList<>(allColumns);
        String columnListStr = String.join(", ", columnList);
        String placeholderRow = "(" + columnList.stream().map(c -> "?").collect(Collectors.joining(", ")) + ")";
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName)
                  .append(" (").append(columnListStr).append(") VALUES ");
        
        List<String> valuePlaceholders = new ArrayList<>();
        List<Object> allParams = new ArrayList<>();
        
        for (Map<String, Object> record : processedRecords) {
            valuePlaceholders.add(placeholderRow);
            
            // Add parameters in the same order as columns
            for (String column : columnList) {
                Object value = record.getOrDefault(column, null);
                allParams.add(value);
            }
        }
        
        sqlBuilder.append(String.join(", ", valuePlaceholders));
        sqlBuilder.append(" RETURNING *");
        
        try {
            List<Map<String, Object>> results = jdbcTemplate.queryForList(sqlBuilder.toString(), allParams.toArray());
            log.debug("Created {} records in table {}", results.size(), tableName);
            return results;
        } catch (Exception e) {
            log.error("Error creating bulk records in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error creating bulk records: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> updateRecord(String tableName, String id, Map<String, Object> data, boolean isPartial) {
        // Validate input
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("ID cannot be empty");
        }
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "UPDATE");

        // Find primary key column
        String primaryKeyColumn = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id"); // fallback to 'id'

        // Validate columns
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());

        for (String column : data.keySet()) {
            if (!validColumns.contains(column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }

        // Build UPDATE query
        List<String> setParts = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!entry.getKey().equals(primaryKeyColumn)) { // Don't update primary key
                setParts.add(entry.getKey() + " = ?");
                values.add(entry.getValue());
            }
        }
        
        if (setParts.isEmpty()) {
            throw new IllegalArgumentException("No valid columns to update");
        }
        
        values.add(convertValueToColumnType(primaryKeyColumn, id, tableInfo)); // Add ID for WHERE clause
        
        // Try with RETURNING first, fall back to separate queries if not supported
        try {
            String query = "UPDATE " + tableName + " SET " + String.join(", ", setParts) + 
                          " WHERE " + primaryKeyColumn + " = ? RETURNING *";
            List<Map<String, Object>> results = jdbcTemplate.queryForList(query, values.toArray());
            return results.isEmpty() ? null : results.get(0);
        } catch (Exception e) {
            log.warn("RETURNING clause not supported in UPDATE, using fallback approach: " + e.getMessage());
            
            // Fallback: Update without RETURNING, then select the record
            String updateQuery = "UPDATE " + tableName + " SET " + String.join(", ", setParts) + 
                               " WHERE " + primaryKeyColumn + " = ?";
            int rowsAffected = jdbcTemplate.update(updateQuery, values.toArray());
            
            if (rowsAffected > 0) {
                // Select the updated record
                String selectQuery = "SELECT * FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?";
                List<Map<String, Object>> results = jdbcTemplate.queryForList(selectQuery, convertValueToColumnType(primaryKeyColumn, id, tableInfo));
                return results.isEmpty() ? null : results.get(0);
            }
            
            return null;
        }
    }

    public boolean deleteRecord(String tableName, String id) {
        // Validate input
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("ID cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = getValidatedTableInfo(tableName);
        validateTablePermission(tableName, "DELETE");

        // Find primary key column
        String primaryKeyColumn = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id"); // fallback to 'id'

        String query = "DELETE FROM " + tableName + " WHERE " + primaryKeyColumn + " = ?";
        int affectedRows = jdbcTemplate.update(query, convertValueToColumnType(primaryKeyColumn, id, tableInfo));
        
        return affectedRows > 0;
    }
    
    /**
     * Validate table name for security - prevents SQL injection
     */
    private void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        // Basic SQL injection protection - only allow alphanumeric and underscore
        if (!tableName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }
    
    /**
     * Validate and get table info with role-based access control
     * Throws exception if table not found or user lacks permissions
     */
    private TableInfo getValidatedTableInfo(String tableName) {
        validateTableName(tableName);
        Map<String, TableInfo> schema = schemaService.getTableSchema();
        TableInfo tableInfo = schema.get(tableName);
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return tableInfo;
    }
    
    /**
     * Check if current user has required permission on table
     */
    private boolean hasTablePermission(String tableName, String permission) {
        try {
            String query = "SELECT has_table_privilege(current_user, ?, ?)";
            Boolean hasPermission = jdbcTemplate.queryForObject(query, Boolean.class, tableName, permission);
            return hasPermission != null && hasPermission;
        } catch (Exception e) {
            log.warn("Failed to check table permission for {}: {}", tableName, e.getMessage());
            return true; // Fallback to allowing access if permission check fails
        }
    }
    
    /**
     * Validate table permissions for specific operations
     */
    private void validateTablePermission(String tableName, String operation) {
        String permission = switch (operation.toLowerCase()) {
            case "select", "read" -> "SELECT";
            case "insert", "create" -> "INSERT";
            case "update", "patch", "put" -> "UPDATE";
            case "delete" -> "DELETE";
            default -> "SELECT"; // Default to most restrictive
        };
        
        if (!hasTablePermission(tableName, permission)) {
            throw new IllegalArgumentException("Access denied: insufficient privileges for " + operation + " on table " + tableName);
        }
    }
    
    /**
     * Validate enum value against PostgreSQL enum type
     */
    private String validateEnumValue(String enumTypeName, String value) {
        try {
            List<String> validValues = schemaService.getEnumValues(enumTypeName);
            if (!validValues.isEmpty() && !validValues.contains(value)) {
                throw new IllegalArgumentException("Invalid enum value '" + value + "' for type '" + enumTypeName + "'. Valid values: " + validValues);
            }
            return value;
        } catch (Exception e) {
            log.warn("Failed to validate enum value '{}' for type '{}': {}", value, enumTypeName, e.getMessage());
            return value; // Fallback to allowing the value
        }
    }
    
    /**
     * Validate network address format (INET/CIDR)
     */
    private String validateNetworkAddress(String value) {
        // Basic validation for IPv4/IPv6 addresses and CIDR notation
        if (value.matches("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\/([0-9]|[1-2][0-9]|3[0-2]))?$") ||
            value.matches("^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}(\\/([0-9]|[1-9][0-9]|1[0-1][0-9]|12[0-8]))?$") ||
            value.matches("^::1(\\/128)?$") || value.matches("^::(\\/0)?$")) {
            return value;
        }
        throw new IllegalArgumentException("Invalid network address format: " + value);
    }
    
    /**
     * Validate MAC address format
     */
    private String validateMacAddress(String value) {
        // MAC address patterns: XX:XX:XX:XX:XX:XX or XX-XX-XX-XX-XX-XX
        if (value.matches("^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$") ||
            value.matches("^([0-9A-Fa-f]{2}[:-]){7}([0-9A-Fa-f]{2})$")) { // MACADDR8 has 8 bytes
            return value;
        }
        throw new IllegalArgumentException("Invalid MAC address format: " + value);
    }
    
    /**
     * Expand relationships in records based on expand parameter
     * Supports syntax: "customer" or "customer(limit:5)" or "customer,orders"
     */
    private List<Map<String, Object>> expandRelationships(List<Map<String, Object>> records, TableInfo tableInfo, String expand) {
        if (records.isEmpty()) {
            return records;
        }
        
        String[] expansions = expand.split(",");
        for (String expansion : expansions) {
            expansion = expansion.trim();
            if (expansion.isEmpty()) {
                continue;
            }
            
            // Parse expansion (e.g., "customer(limit:5)" -> "customer", limit=5)
            String relationshipName = expansion;
            Map<String, String> expansionParams = new HashMap<>();
            
            if (expansion.contains("(") && expansion.endsWith(")")) {
                int parenIndex = expansion.indexOf("(");
                relationshipName = expansion.substring(0, parenIndex);
                String paramsStr = expansion.substring(parenIndex + 1, expansion.length() - 1);
                
                // Parse parameters like "limit:5,select:name"
                String[] paramPairs = paramsStr.split(",");
                for (String pair : paramPairs) {
                    String[] keyValue = pair.split(":");
                    if (keyValue.length == 2) {
                        expansionParams.put(keyValue[0].trim(), keyValue[1].trim());
                    }
                }
            }
            
            expandSingleRelationship(records, tableInfo, relationshipName, expansionParams);
        }
        
        return records;
    }
    
    /**
     * Expand a single relationship for all records
     */
    private void expandSingleRelationship(List<Map<String, Object>> records, TableInfo tableInfo, 
                                        String relationshipName, Map<String, String> expansionParams) {
        try {
            Map<String, TableInfo> allTables = schemaService.getTableSchema();
            
            // Check if it's a forward relationship (FK in current table)
            for (var fk : tableInfo.getForeignKeys()) {
                if (fk.getReferencedTable().equalsIgnoreCase(relationshipName)) {
                    expandForwardRelationship(records, fk, expansionParams);
                    return;
                }
            }
            
            // Check if it's a reverse relationship (FK in other table pointing to this table)
            for (var otherTableEntry : allTables.entrySet()) {
                String otherTableName = otherTableEntry.getKey();
                TableInfo otherTableInfo = otherTableEntry.getValue();
                
                if (otherTableName.equalsIgnoreCase(relationshipName)) {
                    // Look for FK in other table pointing to current table
                    for (var otherFk : otherTableInfo.getForeignKeys()) {
                        if (otherFk.getReferencedTable().equalsIgnoreCase(tableInfo.getName())) {
                            expandReverseRelationship(records, tableInfo, otherTableName, otherFk, expansionParams);
                            return;
                        }
                    }
                }
            }
            
            log.warn("Relationship '{}' not found for table '{}'", relationshipName, tableInfo.getName());
        } catch (Exception e) {
            log.error("Error expanding relationship '{}': {}", relationshipName, e.getMessage());
        }
    }
    
    /**
     * Expand forward relationship (Many-to-One): current table has FK to referenced table
     * Uses batch loading to prevent N+1 queries
     */
    private void expandForwardRelationship(List<Map<String, Object>> records, ForeignKeyInfo fk, Map<String, String> expansionParams) {
        String referencedTable = fk.getReferencedTable();
        String foreignKeyColumn = fk.getColumnName();
        String referencedColumn = fk.getReferencedColumn();
        
        // Collect all foreign key values
        Set<Object> fkValues = records.stream()
            .map(record -> record.get(foreignKeyColumn))
            .filter(java.util.Objects::nonNull)
            .collect(Collectors.toSet());
        
        if (fkValues.isEmpty()) {
            return;
        }
        
        // Use batch loader for efficient querying
        String selectClause = expansionParams.getOrDefault("select", "*");
        Map<Object, Map<String, Object>> relatedLookup = batchLoader.loadSingleRelatedRecords(
            referencedTable, referencedColumn, fkValues, selectClause);
        
        // Add related records to main records
        for (Map<String, Object> record : records) {
            Object fkValue = record.get(foreignKeyColumn);
            if (fkValue != null && relatedLookup.containsKey(fkValue)) {
                record.put(referencedTable, relatedLookup.get(fkValue));
            }
        }
        
        log.debug("Expanded forward relationship {} -> {} for {} records using batch loading", 
                 foreignKeyColumn, referencedTable, records.size());
    }
    
    /**
     * Expand reverse relationship (One-to-Many): other table has FK to current table
     * Uses batch loading to prevent N+1 queries
     */
    private void expandReverseRelationship(List<Map<String, Object>> records, TableInfo currentTableInfo, 
                                         String relatedTableName, ForeignKeyInfo fk, Map<String, String> expansionParams) {
        String currentPkColumn = currentTableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id");
        
        String foreignKeyColumn = fk.getColumnName();
        
        // Collect all primary key values
        Set<Object> pkValues = records.stream()
            .map(record -> record.get(currentPkColumn))
            .filter(java.util.Objects::nonNull)
            .collect(Collectors.toSet());
        
        if (pkValues.isEmpty()) {
            return;
        }
        
        // Use batch loader for efficient querying
        String selectClause = expansionParams.getOrDefault("select", "*");
        int limit = 0; // Default to no limit
        if (expansionParams.containsKey("limit")) {
            try {
                limit = Math.min(Integer.parseInt(expansionParams.get("limit")), MAX_LIMIT);
            } catch (NumberFormatException e) {
                log.warn("Invalid limit parameter: {}", expansionParams.get("limit"));
            }
        }
        
        Map<Object, List<Map<String, Object>>> relatedGrouped = batchLoader.loadRelatedRecords(
            relatedTableName, foreignKeyColumn, currentPkColumn, pkValues, selectClause, limit);
        
        // Add related records to main records
        for (Map<String, Object> record : records) {
            Object pkValue = record.get(currentPkColumn);
            if (pkValue != null && relatedGrouped.containsKey(pkValue)) {
                record.put(relatedTableName, relatedGrouped.get(pkValue));
            } else {
                record.put(relatedTableName, new ArrayList<>());
            }
        }
        
        log.debug("Expanded reverse relationship {} -> {} for {} records using batch loading", 
                 relatedTableName, foreignKeyColumn, records.size());
    }
}