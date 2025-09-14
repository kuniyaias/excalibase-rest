package io.github.excalibase.postgres.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for converting PostgreSQL arrays to Java Lists for JSON serialization.
 * Based on the approach used in the GraphQL module's PostgresTypeConverter.
 */
public class PostgresArrayConverter {
    private static final Logger log = LoggerFactory.getLogger(PostgresArrayConverter.class);
    
    /**
     * Converts PostgreSQL arrays in query results to Java Lists for proper JSON serialization.
     * This prevents Jackson serialization errors with PgArray objects.
     */
    public static List<Map<String, Object>> convertPostgresArrays(List<Map<String, Object>> results) {
        if (results == null || results.isEmpty()) {
            return results;
        }
        
        return results.stream().map(PostgresArrayConverter::convertPostgresArraysInRecord)
                .collect(Collectors.toList());
    }
    
    /**
     * Converts PostgreSQL arrays in a single record to Java Lists.
     */
    public static Map<String, Object> convertPostgresArraysInRecord(Map<String, Object> record) {
        if (record == null || record.isEmpty()) {
            return record;
        }
        
        Map<String, Object> convertedRecord = new HashMap<>(record);
        
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Array) {
                try {
                    List<Object> convertedArray = convertSqlArrayToList((Array) value);
                    convertedRecord.put(entry.getKey(), convertedArray);
                    log.debug("Converted PostgreSQL array for column '{}' with {} elements", 
                             entry.getKey(), convertedArray.size());
                } catch (Exception e) {
                    log.error("Failed to convert PostgreSQL array for column '{}': {}", 
                             entry.getKey(), e.getMessage());
                    convertedRecord.put(entry.getKey(), null);
                }
            }
        }
        
        return convertedRecord;
    }
    
    /**
     * Converts a SQL Array to a Java List.
     */
    private static List<Object> convertSqlArrayToList(Array sqlArray) throws SQLException {
        if (sqlArray == null) {
            return Collections.emptyList();
        }
        
        try {
            Object[] elements = (Object[]) sqlArray.getArray();
            if (elements == null) {
                return Collections.emptyList();
            }
            
            return Arrays.stream(elements)
                    .collect(Collectors.toList());
                    
        } catch (SQLException e) {
            // If connection is closed, try to parse string representation
            log.debug("SQL Array access failed (likely connection closed), trying string representation: {}", 
                     e.getMessage());
            return parsePostgresArrayString(sqlArray.toString());
        }
    }
    
    /**
     * Parse PostgreSQL array string representation like "{apple,banana,cherry}" or "{1,2,3,4,5}".
     */
    private static List<Object> parsePostgresArrayString(String arrayStr) {
        if (arrayStr == null || arrayStr.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        String trimmed = arrayStr.trim();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        
        if (trimmed.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Handle quoted string arrays vs unquoted numeric arrays
        if (trimmed.contains("\"")) {
            return parseQuotedArrayElements(trimmed);
        } else {
            return Arrays.stream(trimmed.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
        }
    }
    
    /**
     * Parse quoted array elements like "apple","banana","cherry".
     */
    private static List<Object> parseQuotedArrayElements(String arrayContent) {
        List<Object> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;
        
        for (char c : arrayContent.toCharArray()) {
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                if (current.length() > 0) {
                    elements.add(current.toString().trim());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        
        if (current.length() > 0) {
            elements.add(current.toString().trim());
        }
        
        return elements;
    }
}