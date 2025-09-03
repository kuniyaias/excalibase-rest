package io.github.excalibase.service

import io.github.excalibase.model.ColumnInfo
import io.github.excalibase.model.ForeignKeyInfo
import io.github.excalibase.model.TableInfo
import io.github.excalibase.service.QueryComplexityService
import io.github.excalibase.service.RelationshipBatchLoader
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Specification
import spock.lang.Subject

class RestApiServiceTest extends Specification {

    def jdbcTemplate = Mock(JdbcTemplate)
    def schemaService = Mock(DatabaseSchemaService)
    def complexityService = Mock(QueryComplexityService)
    def batchLoader = Mock(RelationshipBatchLoader)
    
    @Subject
    RestApiService restApiService = new RestApiService(jdbcTemplate, schemaService, complexityService, batchLoader)
    
    def setup() {
        // Mock all permission checks to return true by default
        jdbcTemplate.queryForObject("SELECT has_table_privilege(current_user, ?, ?)", Boolean.class, _, _) >> true
    }

    def "should get records with basic pagination"() {
        given: "a valid table with columns"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock query results"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "email": "john@example.com"],
            ["id": 2, "name": "Jane", "email": "jane@example.com"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 10L

        when: "getting records with pagination"
        def params = new LinkedMultiValueMap<String, String>()
        def result = restApiService.getRecords("users", params, 0, 5, null, "asc", null, null)

        then: "should return paginated data"
        result.data.size() == 2
        result.pagination.offset == 0
        result.pagination.limit == 5
        result.pagination.total == 10
        result.pagination.hasMore == true
    }

    def "should apply filters correctly"() {
        given: "a table with columns and filter parameters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("name", "like.John")
        params.add("age", "gt.18")
        
        and: "mock query results for both main query and count query"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >>> [
            [["id": 1, "name": "John"]], // First call: main query
            [["id": 1, "name": "John"]]  // Second call: if called again
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with filters"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should apply WHERE clause with filters"
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("WHERE") && sql.contains("name LIKE ?") && sql.contains("age > ?")
        }, _ as Object[]) >> [["id": 1, "name": "John"]]
        
        and: "should return filtered data"
        result.data != null
        result.data.size() == 1
        result.data[0].id == 1
        result.data[0].name == "John"
    }

    def "should handle OR conditions"() {
        given: "a table with OR filter parameters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("or", "(name.like.John,age.gt.65)")
        
        and: "mock query results"
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with OR conditions"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should apply OR logic in WHERE clause"
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("WHERE") && sql.contains("(") && sql.contains("OR") && sql.contains(")")
        }, _ as Object[]) >> [["id": 1, "name": "John"]]
        
        and: "should return data with OR conditions"
        result.data != null
        result.data.size() == 1
        result.data[0].id == 1
        result.data[0].name == "John"
    }

    def "should validate table name and throw exception for invalid table"() {
        given: "schema service returns empty schema"
        schemaService.getTableSchema() >> [:]

        when: "trying to access non-existent table"
        restApiService.getRecords("invalid_table", new LinkedMultiValueMap(), 0, 10, null, "asc", null, null)

        then: "should throw IllegalArgumentException"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Table not found: invalid_table")
    }

    def "should validate pagination limits"() {
        when: "using invalid offset"
        restApiService.getRecords("users", new LinkedMultiValueMap(), -1, 10, null, "asc", null, null)

        then: "should throw exception for negative offset"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Offset must be between 0 and")

        when: "using excessive limit"
        restApiService.getRecords("users", new LinkedMultiValueMap(), 0, 2000, null, "asc", null, null)

        then: "should throw exception for excessive limit"
        def exception2 = thrown(IllegalArgumentException)
        exception2.message.contains("Limit must be between 1 and")
    }

    def "should handle column selection"() {
        given: "a table with select parameter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock query results"
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with column selection"
        def result = restApiService.getRecords("users", new LinkedMultiValueMap(), 0, 10, null, "asc", "id,name", null)

        then: "should use SELECT with specified columns"
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("SELECT id,name") && !sql.contains("SELECT *")
        }, _ as Object[]) >> [["id": 1, "name": "John"]]
        
        and: "should return selected data"
        result.data != null
        result.data.size() == 1
        result.data[0].id == 1
        result.data[0].name == "John"
    }

    def "should validate column names in select parameter"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]

        when: "using invalid column in select"
        restApiService.getRecords("users", new LinkedMultiValueMap(), 0, 10, null, "asc", "invalid_column", null)

        then: "should throw exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Invalid column: invalid_column")
    }

    def "should create single record"() {
        given: "a valid table and data"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["name": "John", "email": "john@example.com"]
        
        and: "mock successful insert with RETURNING"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 1, "name": "John", "email": "john@example.com"]]

        when: "creating a record"
        def result = restApiService.createRecord("users", data)

        then: "should return created record"
        result.id == 1
        result.name == "John"
        result.email == "john@example.com"
    }

    def "should validate data columns on create"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["invalid_column": "value"]

        when: "creating record with invalid column"
        restApiService.createRecord("users", data)

        then: "should throw exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Invalid column: invalid_column")
    }

    def "should update record"() {
        given: "a valid table and update data"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["name": "John Updated"]
        
        and: "mock successful update with RETURNING"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 1, "name": "John Updated", "email": "john@example.com"]]

        when: "updating a record"
        def result = restApiService.updateRecord("users", "1", data, false)

        then: "should return updated record"
        result.id == 1
        result.name == "John Updated"
    }

    def "should delete record"() {
        given: "a valid table"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock successful delete"
        jdbcTemplate.update(_ as String, _ as Object) >> 1

        when: "deleting a record"
        def result = restApiService.deleteRecord("users", "1")

        then: "should return true for successful deletion"
        result == true
    }

    def "should return false for delete when record not found"() {
        given: "a valid table"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock unsuccessful delete (no rows affected)"
        jdbcTemplate.update(_ as String, _ as Object) >> 0

        when: "deleting non-existent record"
        def result = restApiService.deleteRecord("users", "999")

        then: "should return false"
        result == false
    }

    def "should handle cursor-based pagination"() {
        given: "a valid table"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock query results"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 2, "name": "Jane"],
            ["id": 3, "name": "Bob"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 10L

        when: "getting records with cursor pagination"
        def result = restApiService.getRecordsWithCursor("users", new LinkedMultiValueMap(), 
                                                        "2", "eyJpZCI6MX0=", null, null,
                                                        "id", "asc", null, null)

        then: "should return edges with cursors"
        result.edges.size() == 2
        result.pageInfo.hasNextPage != null
        result.pageInfo.hasPreviousPage != null
        result.totalCount == 10
    }

    def "should convert values to correct column types"() {
        given: "a table with different column types"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("price", "decimal", false, true),
            new ColumnInfo("active", "boolean", false, false),
            new ColumnInfo("name", "varchar", false, true)
        ]
        def tableInfo = new TableInfo("products", columns, [])
        schemaService.getTableSchema() >> ["products": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("id", "eq.123")
        params.add("price", "gte.99.99")
        params.add("active", "is.true")
        params.add("name", "like.test")
        
        and: "mock query results"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> []
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 0L

        when: "applying filters with type conversion"
        restApiService.getRecords("products", params, 0, 10, null, "asc", null, null)

        then: "should convert parameters to correct types"
        1 * jdbcTemplate.queryForList(_ as String, { Object[] args ->
            args[0] instanceof Integer && args[0] == 123 &&           // id converted to Integer
            args[1] instanceof Double && args[1] == 99.99 &&          // price converted to Double
            args[2] instanceof Boolean && args[2] == true &&          // active converted to Boolean
            args[3] instanceof String && args[3] == "%test%"          // name kept as String with LIKE wildcards
        })
    }

    private TableInfo createSampleTableInfo() {
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("email", "varchar", false, false),
            new ColumnInfo("age", "integer", false, true)
        ]
        return new TableInfo("users", columns, [])
    }
}