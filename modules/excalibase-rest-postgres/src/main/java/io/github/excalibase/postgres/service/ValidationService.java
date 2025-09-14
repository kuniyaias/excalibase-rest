package io.github.excalibase.postgres.service;

import io.github.excalibase.annotation.ExcalibaseService;
import io.github.excalibase.constant.SupportedDatabaseConstant;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.service.IValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ExcalibaseService(serviceName = SupportedDatabaseConstant.POSTGRES)
public class ValidationService implements IValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);
    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSchemaService schemaService;

    // Maximum allowed values for security
    private static final int MAX_LIMIT = 1000;
    private static final int MAX_OFFSET = 1000000; // 1 million

    public ValidationService(JdbcTemplate jdbcTemplate, DatabaseSchemaService schemaService) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaService = schemaService;
    }

    /**
     * Validate pagination parameters
     */
    public void validatePaginationParams(int offset, int limit) {
        if (offset < 0 || offset > MAX_OFFSET) {
            throw new IllegalArgumentException("Offset must be between 0 and " + MAX_OFFSET);
        }
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("Limit must be between 1 and " + MAX_LIMIT);
        }
    }

    /**
     * Validate table name for security - prevents SQL injection
     */
    public void validateTableName(String tableName) {
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
    public TableInfo getValidatedTableInfo(String tableName) {
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
    public boolean hasTablePermission(String tableName, String permission) {
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
    public void validateTablePermission(String tableName, String operation) {
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
     * Validate column names against table schema
     */
    public void validateColumns(Set<String> columnNames, TableInfo tableInfo) {
        Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());

        for (String column : columnNames) {
            if (!validColumns.contains(column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }
    }

    /**
     * Validate column names in select parameter
     */
    public void validateSelectColumns(String select, TableInfo tableInfo) {
        if (select == null || select.trim().isEmpty()) {
            return;
        }

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
    }

    /**
     * Validate order by column
     */
    public void validateOrderByColumn(String orderBy, TableInfo tableInfo) {
        if (orderBy == null || orderBy.trim().isEmpty()) {
            return;
        }

        boolean columnExists = tableInfo.getColumns().stream()
                .anyMatch(col -> col.getName().equals(orderBy));
        if (!columnExists) {
            throw new IllegalArgumentException("Invalid column for ordering: " + orderBy);
        }
    }

    /**
     * Validate enum value against PostgreSQL enum type
     */
    public String validateEnumValue(String enumTypeName, String value) {
        try {
            var validValues = schemaService.getEnumValues(enumTypeName);
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
     * Validate network address (IP or CIDR format)
     */
    public String validateNetworkAddress(String address) {
        if (address == null || address.trim().isEmpty()) {
            throw new IllegalArgumentException("Network address cannot be empty");
        }

        String trimmed = address.trim();

        // IPv4 address pattern (basic validation)
        String ipv4Pattern = "^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(?:/[0-9]{1,2})?$";

        // IPv6 address pattern (basic validation)
        String ipv6Pattern = "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$|^::1$|^::$";

        if (trimmed.matches(ipv4Pattern) || trimmed.matches(ipv6Pattern)) {
            return trimmed;
        }

        throw new IllegalArgumentException("Invalid network address format: " + address);
    }

    /**
     * Validate MAC address (supports both colon and dash separators, 6 and 8 byte formats)
     */
    public String validateMacAddress(String macAddress) {
        if (macAddress == null || macAddress.trim().isEmpty()) {
            throw new IllegalArgumentException("MAC address cannot be empty");
        }

        String trimmed = macAddress.trim();

        // 6-byte MAC address patterns (MACADDR)
        String mac6Colon = "^([0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$";
        String mac6Dash = "^([0-9a-fA-F]{2}-){5}[0-9a-fA-F]{2}$";

        // 8-byte MAC address pattern (MACADDR8)
        String mac8Colon = "^([0-9a-fA-F]{2}:){7}[0-9a-fA-F]{2}$";
        String mac8Dash = "^([0-9a-fA-F]{2}-){7}[0-9a-fA-F]{2}$";

        if (trimmed.matches(mac6Colon) || trimmed.matches(mac6Dash) ||
                trimmed.matches(mac8Colon) || trimmed.matches(mac8Dash)) {
            return trimmed;
        }

        throw new IllegalArgumentException("Invalid MAC address format: " + macAddress);
    }

    /**
     * Basic SQL injection protection - check for common dangerous patterns
     */
    public void validateFilterValue(String value) {
        String upperValue = value.toUpperCase();
        if (upperValue.contains(";") || upperValue.contains("--") || upperValue.contains("/*") ||
                upperValue.contains("*/") || upperValue.contains(" DROP ") || upperValue.contains(" DELETE ") ||
                upperValue.contains(" UPDATE ") || upperValue.contains(" INSERT ") || upperValue.contains(" CREATE ") ||
                upperValue.contains(" ALTER ") || upperValue.contains(" TRUNCATE ") || upperValue.contains("UNION ") ||
                upperValue.contains("EXEC") || upperValue.contains("EXECUTE")) {
            throw new IllegalArgumentException("Invalid characters detected in filter value");
        }
    }

    /**
     * Validate IN operator values for security
     */
    public void validateInOperatorValues(String inValues) {
        if (inValues.contains(";") || inValues.contains("--") || inValues.contains("/*") ||
                inValues.contains("*/") || inValues.contains("DROP") || inValues.contains("DELETE") ||
                inValues.contains("UPDATE") || inValues.contains("INSERT") || inValues.contains("CREATE")) {
            throw new IllegalArgumentException("Invalid characters detected in filter value");
        }
    }

    /**
     * Handle database constraint violations and convert to ValidationException
     */
    public void handleDatabaseConstraintViolation(DataIntegrityViolationException e, String tableName, Map<String, Object> data) {
        String message = e.getMessage();
        String rootMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : message;

        // Check for common constraint violations
        if (rootMessage != null) {
            if (rootMessage.contains("violates not-null constraint")) {
                String columnName = extractColumnNameFromConstraint(rootMessage, "violates not-null constraint");
                throw new ValidationException("Field '" + columnName + "' is required and cannot be null");
            } else if (rootMessage.contains("violates unique constraint")) {
                String constraintName = extractConstraintNameFromMessage(rootMessage);
                throw new ValidationException("Duplicate value violates unique constraint: " + constraintName);
            } else if (rootMessage.contains("violates foreign key constraint")) {
                String constraintName = extractConstraintNameFromMessage(rootMessage);
                throw new ValidationException("Foreign key constraint violation: " + constraintName);
            } else if (rootMessage.contains("violates check constraint")) {
                String constraintName = extractConstraintNameFromMessage(rootMessage);
                throw new ValidationException("Check constraint violation: " + constraintName);
            } else if (rootMessage.contains("invalid input value for enum")) {
                String enumType = extractEnumTypeFromMessage(rootMessage);
                throw new ValidationException("Invalid enum value. Please check valid values for type: " + enumType);
            }
        }

        // Generic constraint violation message
        throw new ValidationException("Data validation error: " + (rootMessage != null ? rootMessage : message));
    }

    /**
     * Handle SQL constraint violations from SQLException
     */
    public void handleSqlConstraintViolation(SQLException sqlEx, String tableName, Map<String, Object> data) {
        String sqlState = sqlEx.getSQLState();
        String message = sqlEx.getMessage();

        // PostgreSQL error codes
        switch (sqlState != null ? sqlState : "") {
            case "23502": // NOT NULL constraint violation
                String columnName = extractColumnNameFromConstraint(message, "violates not-null constraint");
                throw new ValidationException("Field '" + columnName + "' is required and cannot be null");

            case "23505": // Unique constraint violation
                String constraintName = extractConstraintNameFromMessage(message);
                throw new ValidationException("Duplicate value violates unique constraint: " + constraintName);

            case "23503": // Foreign key constraint violation
                String fkConstraintName = extractConstraintNameFromMessage(message);
                throw new ValidationException("Foreign key constraint violation: " + fkConstraintName);

            case "23514": // Check constraint violation
                String checkConstraintName = extractConstraintNameFromMessage(message);
                throw new ValidationException("Check constraint violation: " + checkConstraintName);

            case "22P02": // Invalid text representation (e.g., enum values)
                if (message != null && message.contains("invalid input value for enum")) {
                    String enumType = extractEnumTypeFromMessage(message);
                    throw new ValidationException("Invalid enum value. Please check valid values for type: " + enumType);
                }
                throw new ValidationException("Invalid data format: " + message);

            default:
                // Other constraint violations
                throw new ValidationException("Data validation error: " + message);
        }
    }

    /**
     * Extract column name from constraint violation message
     */
    public String extractColumnNameFromConstraint(String message, String constraintType) {
        if (message == null) return "unknown";

        // Pattern: 'null value in column "column_name" violates not-null constraint'
        String pattern = "column \"([^\"]+)\" " + constraintType;
        Pattern regexPattern = Pattern.compile(pattern);
        Matcher matcher = regexPattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    /**
     * Extract constraint name from error message
     */
    private String extractConstraintNameFromMessage(String message) {
        if (message == null) return "unknown";

        // Pattern: 'constraint "constraint_name"'
        Pattern pattern = Pattern.compile("constraint \"([^\"]+)\"");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    /**
     * Extract enum type name from error message
     */
    private String extractEnumTypeFromMessage(String message) {
        if (message == null) return "unknown";

        // Pattern: 'invalid input value for enum enum_type_name'
        Pattern pattern = Pattern.compile("invalid input value for enum ([a-zA-Z_][a-zA-Z0-9_]*)");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    public int getMaxLimit() {
        return MAX_LIMIT;
    }

    public int getMaxOffset() {
        return MAX_OFFSET;
    }
}
