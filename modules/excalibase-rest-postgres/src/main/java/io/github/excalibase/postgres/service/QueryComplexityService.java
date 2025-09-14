package io.github.excalibase.postgres.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

/**
 * Service to analyze and limit query complexity to prevent expensive operations
 */
@Service
public class QueryComplexityService {

    private static final Logger log = LoggerFactory.getLogger(QueryComplexityService.class);
    
    // Configuration for complexity limits
    private final int maxComplexityScore;
    private final int maxDepth;
    private final int maxBreadth;
    private final boolean complexityAnalysisEnabled;
    
    public QueryComplexityService(
            @Value("${app.query.max-complexity-score:1000}") int maxComplexityScore,
            @Value("${app.query.max-depth:10}") int maxDepth,
            @Value("${app.query.max-breadth:50}") int maxBreadth,
            @Value("${app.query.complexity-analysis-enabled:true}") boolean complexityAnalysisEnabled) {
        this.maxComplexityScore = maxComplexityScore;
        this.maxDepth = maxDepth;
        this.maxBreadth = maxBreadth;
        this.complexityAnalysisEnabled = complexityAnalysisEnabled;
    }
    
    /**
     * Analyze query complexity and throw exception if limits are exceeded
     */
    public void validateQueryComplexity(String tableName, MultiValueMap<String, String> params, 
                                      int limit, String expand) {
        if (!complexityAnalysisEnabled) {
            return;
        }
        
        try {
            QueryAnalysis analysis = analyzeQuery(tableName, params, limit, expand);
            
            if (analysis.complexityScore > maxComplexityScore) {
                throw new IllegalArgumentException(
                    String.format("Query complexity score %d exceeds maximum allowed %d", 
                                analysis.complexityScore, maxComplexityScore));
            }
            
            if (analysis.depth > maxDepth) {
                throw new IllegalArgumentException(
                    String.format("Query depth %d exceeds maximum allowed %d", 
                                analysis.depth, maxDepth));
            }
            
            if (analysis.breadth > maxBreadth) {
                throw new IllegalArgumentException(
                    String.format("Query breadth %d exceeds maximum allowed %d", 
                                analysis.breadth, maxBreadth));
            }
            
            log.debug("Query complexity validation passed: score={}, depth={}, breadth={}", 
                     analysis.complexityScore, analysis.depth, analysis.breadth);
                     
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                throw e; // Re-throw validation errors
            }
            log.warn("Query complexity analysis failed: {}", e.getMessage());
            // Allow query to proceed if analysis fails
        }
    }
    
    /**
     * Analyze query complexity without throwing exceptions
     */
    public QueryAnalysis analyzeQuery(String tableName, MultiValueMap<String, String> params, 
                                    int limit, String expand) {
        QueryAnalysis analysis = new QueryAnalysis();
        
        // Base complexity for table access
        analysis.complexityScore += 10;
        analysis.depth = 1;
        analysis.breadth = 1;
        
        // Analyze limit impact
        analysis.complexityScore += Math.min(limit, 100); // Cap limit impact at 100
        
        // Analyze filters complexity
        if (params != null) {
            analysis.breadth += params.size();
            analysis.complexityScore += analyzeFiltersComplexity(params);
        }
        
        // Analyze relationship expansion complexity
        if (expand != null && !expand.trim().isEmpty()) {
            ExpansionAnalysis expansionAnalysis = analyzeExpansionComplexity(expand);
            analysis.depth += expansionAnalysis.depth;
            analysis.breadth += expansionAnalysis.breadth;
            analysis.complexityScore += expansionAnalysis.complexityScore;
        }
        
        return analysis;
    }
    
    /**
     * Analyze complexity of filters
     */
    private int analyzeFiltersComplexity(MultiValueMap<String, String> params) {
        int complexity = 0;
        
        for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            String key = entry.getKey();
            List<String> values = entry.getValue();
            
            // OR conditions are more expensive
            if (key.equals("or")) {
                complexity += values.size() * 15; // Higher cost for OR
            } else {
                complexity += values.size() * 5; // Base cost per filter
            }
            
            // Analyze specific operators
            for (String value : values) {
                if (value.contains("like") || value.contains("ilike")) {
                    complexity += 10; // LIKE operations are expensive
                } else if (value.contains("in.") || value.contains("notin.")) {
                    // Count items in IN clause
                    String inValues = extractInValues(value);
                    int itemCount = inValues.split(",").length;
                    complexity += itemCount * 2;
                } else if (isJsonOperator(value)) {
                    complexity += 8; // JSON operations are moderately expensive
                }
            }
        }
        
        return complexity;
    }
    
    /**
     * Analyze complexity of relationship expansions
     */
    private ExpansionAnalysis analyzeExpansionComplexity(String expand) {
        ExpansionAnalysis analysis = new ExpansionAnalysis();
        
        String[] expansions = expand.split(",");
        analysis.breadth = expansions.length;
        
        for (String expansion : expansions) {
            expansion = expansion.trim();
            if (expansion.isEmpty()) continue;
            
            // Base cost for each expansion
            analysis.complexityScore += 20;
            
            // Check for nested expansions (not implemented yet, but plan for future)
            int nestingLevel = countNestingLevel(expansion);
            analysis.depth = Math.max(analysis.depth, nestingLevel);
            analysis.complexityScore += nestingLevel * 10;
            
            // Analyze expansion parameters
            if (expansion.contains("(") && expansion.contains(")")) {
                String params = extractExpansionParams(expansion);
                if (params.contains("limit:")) {
                    int limitValue = extractLimitFromParams(params);
                    analysis.complexityScore += Math.min(limitValue, 50); // Cap at 50
                }
            }
        }
        
        return analysis;
    }
    
    /**
     * Extract values from IN clause
     */
    private String extractInValues(String value) {
        int start = value.indexOf('(');
        int end = value.indexOf(')', start);
        if (start >= 0 && end > start) {
            return value.substring(start + 1, end);
        }
        return "";
    }
    
    /**
     * Check if operator is a JSON-specific operator
     */
    private boolean isJsonOperator(String value) {
        return value.contains("haskey") || value.contains("haskeys") || 
               value.contains("jsoncontains") || value.contains("@>");
    }
    
    /**
     * Count nesting level in expansion (for future nested expansion support)
     */
    private int countNestingLevel(String expansion) {
        // For now, all expansions are depth 1
        // This can be enhanced when nested expansions are supported
        return 1;
    }
    
    /**
     * Extract parameters from expansion string
     */
    private String extractExpansionParams(String expansion) {
        int start = expansion.indexOf('(');
        int end = expansion.lastIndexOf(')');
        if (start >= 0 && end > start) {
            return expansion.substring(start + 1, end);
        }
        return "";
    }
    
    /**
     * Extract limit value from expansion parameters
     */
    private int extractLimitFromParams(String params) {
        try {
            String[] pairs = params.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":");
                if (keyValue.length == 2 && keyValue[0].trim().equals("limit")) {
                    return Integer.parseInt(keyValue[1].trim());
                }
            }
        } catch (NumberFormatException e) {
            // Ignore parsing errors
        }
        return 10; // Default limit
    }
    
    /**
     * Query analysis result
     */
    public static class QueryAnalysis {
        public int complexityScore = 0;
        public int depth = 0;
        public int breadth = 0;
        
        @Override
        public String toString() {
            return String.format("QueryAnalysis{score=%d, depth=%d, breadth=%d}", 
                               complexityScore, depth, breadth);
        }
    }
    
    /**
     * Expansion analysis result
     */
    private static class ExpansionAnalysis {
        public int complexityScore = 0;
        public int depth = 0;
        public int breadth = 0;
    }
    
    /**
     * Get current complexity limits for monitoring/debugging
     */
    public Map<String, Object> getComplexityLimits() {
        return Map.of(
            "maxComplexityScore", maxComplexityScore,
            "maxDepth", maxDepth,
            "maxBreadth", maxBreadth,
            "analysisEnabled", complexityAnalysisEnabled
        );
    }
}