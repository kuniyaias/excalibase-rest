package io.github.excalibase.postgres.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service to batch relationship queries and prevent N+1 query problems
 * Implements a pattern similar to GraphQL DataLoader
 */
@Service
public class RelationshipBatchLoader {
    
    private static final Logger log = LoggerFactory.getLogger(RelationshipBatchLoader.class);
    
    private final JdbcTemplate jdbcTemplate;
    
    // Cache for batched queries within a single request context
    private final ThreadLocal<Map<String, Map<Object, Object>>> batchCache = ThreadLocal.withInitial(HashMap::new);
    private final ThreadLocal<Map<String, List<BatchRequest>>> pendingBatches = ThreadLocal.withInitial(HashMap::new);
    
    public RelationshipBatchLoader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Load related records for a set of foreign key values
     * Uses batching to minimize database queries
     */
    public Map<Object, List<Map<String, Object>>> loadRelatedRecords(
            String relatedTable, String foreignKeyColumn, String referencedColumn, 
            Set<Object> foreignKeyValues, String selectClause, int limit) {
        
        if (foreignKeyValues.isEmpty()) {
            return Map.of();
        }
        
        String cacheKey = buildCacheKey(relatedTable, foreignKeyColumn, referencedColumn, selectClause, limit);
        
        // Check cache first
        Map<Object, Object> cache = batchCache.get().computeIfAbsent(cacheKey, k -> new ConcurrentHashMap<>());
        
        // Find values not in cache
        Set<Object> uncachedValues = foreignKeyValues.stream()
            .filter(value -> !cache.containsKey(value))
            .collect(Collectors.toSet());
        
        if (!uncachedValues.isEmpty()) {
            // Batch load uncached values
            Map<Object, List<Map<String, Object>>> batchResults = executeBatchQuery(
                relatedTable, foreignKeyColumn, referencedColumn, uncachedValues, selectClause, limit);
            
            // Cache the results
            for (Map.Entry<Object, List<Map<String, Object>>> entry : batchResults.entrySet()) {
                cache.put(entry.getKey(), entry.getValue());
            }
        }
        
        // Return results for all requested values
        Map<Object, List<Map<String, Object>>> results = new HashMap<>();
        for (Object value : foreignKeyValues) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> cachedResult = (List<Map<String, Object>>) cache.get(value);
            results.put(value, cachedResult != null ? cachedResult : List.of());
        }
        
        return results;
    }
    
    /**
     * Load a single related record for a set of foreign key values (for many-to-one relationships)
     */
    public Map<Object, Map<String, Object>> loadSingleRelatedRecords(
            String relatedTable, String referencedColumn, Set<Object> foreignKeyValues, String selectClause) {
        
        if (foreignKeyValues.isEmpty()) {
            return Map.of();
        }
        
        String cacheKey = buildCacheKey(relatedTable, "single", referencedColumn, selectClause, 1);
        
        // Check cache first
        Map<Object, Object> cache = batchCache.get().computeIfAbsent(cacheKey, k -> new ConcurrentHashMap<>());
        
        // Find values not in cache
        Set<Object> uncachedValues = foreignKeyValues.stream()
            .filter(value -> !cache.containsKey(value))
            .collect(Collectors.toSet());
        
        if (!uncachedValues.isEmpty()) {
            // Batch load uncached values
            Map<Object, Map<String, Object>> batchResults = executeSingleBatchQuery(
                relatedTable, referencedColumn, uncachedValues, selectClause);
            
            // Cache the results
            for (Map.Entry<Object, Map<String, Object>> entry : batchResults.entrySet()) {
                cache.put(entry.getKey(), entry.getValue());
            }
            
            // Cache null results for missing records
            for (Object value : uncachedValues) {
                if (!batchResults.containsKey(value)) {
                    cache.put(value, null);
                }
            }
        }
        
        // Return results for all requested values
        Map<Object, Map<String, Object>> results = new HashMap<>();
        for (Object value : foreignKeyValues) {
            @SuppressWarnings("unchecked")
            Map<String, Object> cachedResult = (Map<String, Object>) cache.get(value);
            if (cachedResult != null) {
                results.put(value, cachedResult);
            }
        }
        
        return results;
    }
    
    /**
     * Execute batched query for one-to-many relationships
     */
    private Map<Object, List<Map<String, Object>>> executeBatchQuery(
            String relatedTable, String foreignKeyColumn, String referencedColumn, 
            Set<Object> foreignKeyValues, String selectClause, int limit) {
        
        try {
            // Create IN clause with placeholders
            String inClause = foreignKeyValues.stream().map(v -> "?").collect(Collectors.joining(","));
            String limitClause = limit > 0 ? " LIMIT " + limit : "";
            
            String query = "SELECT " + selectClause + " FROM " + relatedTable + 
                          " WHERE " + foreignKeyColumn + " IN (" + inClause + ")" + limitClause;
            
            List<Map<String, Object>> allResults = jdbcTemplate.queryForList(query, foreignKeyValues.toArray());
            
            // Group results by foreign key value
            Map<Object, List<Map<String, Object>>> groupedResults = new HashMap<>();
            for (Object fkValue : foreignKeyValues) {
                groupedResults.put(fkValue, new ArrayList<>());
            }
            
            for (Map<String, Object> record : allResults) {
                Object fkValue = record.get(foreignKeyColumn);
                if (groupedResults.containsKey(fkValue)) {
                    groupedResults.get(fkValue).add(record);
                }
            }
            
            log.debug("Batch loaded {} records from {} for {} foreign key values", 
                     allResults.size(), relatedTable, foreignKeyValues.size());
            
            return groupedResults;
            
        } catch (Exception e) {
            log.error("Error executing batch query for table {}: {}", relatedTable, e.getMessage());
            // Return empty results for all requested values
            Map<Object, List<Map<String, Object>>> emptyResults = new HashMap<>();
            for (Object value : foreignKeyValues) {
                emptyResults.put(value, List.of());
            }
            return emptyResults;
        }
    }
    
    /**
     * Execute batched query for many-to-one relationships
     */
    private Map<Object, Map<String, Object>> executeSingleBatchQuery(
            String relatedTable, String referencedColumn, Set<Object> foreignKeyValues, String selectClause) {
        
        try {
            // Create IN clause with placeholders
            String inClause = foreignKeyValues.stream().map(v -> "?").collect(Collectors.joining(","));
            
            String query = "SELECT " + selectClause + " FROM " + relatedTable + 
                          " WHERE " + referencedColumn + " IN (" + inClause + ")";
            
            List<Map<String, Object>> allResults = jdbcTemplate.queryForList(query, foreignKeyValues.toArray());
            
            // Create lookup map by referenced column value
            Map<Object, Map<String, Object>> lookupResults = allResults.stream()
                .collect(Collectors.toMap(
                    record -> record.get(referencedColumn),
                    record -> record,
                    (existing, replacement) -> existing // Keep first if duplicates
                ));
            
            log.debug("Batch loaded {} records from {} for {} foreign key values", 
                     allResults.size(), relatedTable, foreignKeyValues.size());
            
            return lookupResults;
            
        } catch (Exception e) {
            log.error("Error executing single batch query for table {}: {}", relatedTable, e.getMessage());
            return Map.of();
        }
    }
    
    /**
     * Build cache key for batched queries
     */
    private String buildCacheKey(String table, String column1, String column2, String select, int limit) {
        return String.format("%s:%s:%s:%s:%d", table, column1, column2, select, limit);
    }
    
    /**
     * Clear the batch cache (call at end of request)
     */
    public void clearCache() {
        batchCache.get().clear();
        pendingBatches.get().clear();
    }
    
    /**
     * Get cache statistics for monitoring
     */
    public Map<String, Object> getCacheStatistics() {
        Map<String, Map<Object, Object>> cache = batchCache.get();
        return Map.of(
            "cacheSize", cache.size(),
            "cacheKeys", cache.keySet().size(),
            "threadId", Thread.currentThread().getId()
        );
    }
    
    /**
     * Batch request for deferred loading
     */
    private static class BatchRequest {
        final String table;
        final String column;
        final Object value;
        final String select;
        
        BatchRequest(String table, String column, Object value, String select) {
            this.table = table;
            this.column = column;
            this.value = value;
            this.select = select;
        }
    }
}