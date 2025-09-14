package io.github.excalibase.postgres.service

import io.github.excalibase.exception.ValidationException
import io.github.excalibase.model.ColumnInfo
import io.github.excalibase.model.ForeignKeyInfo
import io.github.excalibase.model.TableInfo
import io.github.excalibase.postgres.service.*
import io.github.excalibase.service.FilterService
import io.github.excalibase.service.TypeConversionService
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.util.LinkedMultiValueMap
import spock.lang.Specification
import spock.lang.Subject

import java.sql.SQLException

class RestApiServiceTest extends Specification {

    def jdbcTemplate = Mock(JdbcTemplate)
    def schemaService = Mock(DatabaseSchemaService)
    def complexityService = Mock(QueryComplexityService)
    def batchLoader = Mock(RelationshipBatchLoader)
    def selectParserService = Mock(SelectParserService)
    def enhancedRelationshipService = Mock(EnhancedRelationshipService)
    
    // Use real service instances instead of mocks to allow jdbcTemplate mocks to work
    def validationService
    def typeConversionService
    def filterService
    def queryBuilderService
    def crudService
    def upsertService
    
    @Subject
    RestApiService restApiService
    
    def setup() {
        // Mock all permission checks to return true by default
        jdbcTemplate.queryForObject("SELECT has_table_privilege(current_user, ?, ?)", Boolean.class, _, _) >> true
        
        // Create real service instances with mocked dependencies
        validationService = new ValidationService(jdbcTemplate, schemaService)
        typeConversionService = new TypeConversionService(validationService)
        filterService = new FilterService(validationService, typeConversionService)
        queryBuilderService = new QueryBuilderService(validationService, typeConversionService)
        crudService = new CrudService(jdbcTemplate, validationService, typeConversionService, queryBuilderService)
        upsertService = new UpsertService(jdbcTemplate, validationService, typeConversionService, queryBuilderService)
        
        // Create the RestApiService with real service instances
        restApiService = new RestApiService(jdbcTemplate, schemaService, complexityService, batchLoader, 
                                          selectParserService, enhancedRelationshipService,
                                          validationService, typeConversionService, filterService, 
                                          queryBuilderService, crudService, upsertService, 'postgres' )
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
        
        and: "mock select parser service"
        selectParserService.parseSelect("id,name") >> [
            new io.github.excalibase.model.SelectField("id"),
            new io.github.excalibase.model.SelectField("name")
        ]
        selectParserService.getEmbeddedFields(_) >> []
        
        and: "mock query results"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 1, "name": "John"]]
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

    def "should handle composite primary key for get record"() {
        given: "a table with composite primary key"
        def tableInfo = createCompositeKeyTableInfo()
        schemaService.getTableSchema() >> ["order_items": tableInfo]
        
        and: "mock query result"
        def mockResult = [
            ["order_id": 1, "product_id": 2, "quantity": 5, "price": 99.99]
        ]
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> mockResult

        when: "getting record by composite key"
        def result = restApiService.getRecord("order_items", "1,2", null, null)

        then: "should use composite key in WHERE clause"
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("WHERE order_id = ? AND product_id = ?")
        }, { Object[] args ->
            args.length == 2 && args[0] == 1 && args[1] == 2
        }) >> mockResult
        
        and: "should return the record"
        result != null
        result.order_id == 1
        result.product_id == 2
        result.quantity == 5
    }

    def "should handle composite primary key for update record"() {
        given: "a table with composite primary key"
        def tableInfo = createCompositeKeyTableInfo()
        schemaService.getTableSchema() >> ["order_items": tableInfo]
        
        def updateData = ["quantity": 10]
        
        and: "mock successful update"
        def mockResult = [
            ["order_id": 1, "product_id": 2, "quantity": 10, "price": 99.99]
        ]

        when: "updating record by composite key"
        def result = restApiService.updateRecord("order_items", "1,2", updateData, false)

        then: "should use composite key in WHERE clause"
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("UPDATE order_items SET quantity = ? WHERE order_id = ? AND product_id = ?")
        }, { Object[] args ->
            args.length == 3 && args[0] == 10 && args[1] == 1 && args[2] == 2
        }) >> mockResult
        
        and: "should return updated record"
        result != null
        result.quantity == 10
    }

    def "should handle composite primary key for delete record"() {
        given: "a table with composite primary key"
        def tableInfo = createCompositeKeyTableInfo()
        schemaService.getTableSchema() >> ["order_items": tableInfo]

        when: "deleting record by composite key"
        def result = restApiService.deleteRecord("order_items", "1,2")

        then: "should use composite key in WHERE clause"
        1 * jdbcTemplate.update({ String sql ->
            sql.contains("DELETE FROM order_items WHERE order_id = ? AND product_id = ?")
        }, { Object[] args ->
            args.length == 2 && args[0] == 1 && args[1] == 2
        }) >> 1
        
        and: "should return success"
        result == true
    }

    def "should validate composite key format"() {
        given: "a table with composite primary key"
        def tableInfo = createCompositeKeyTableInfo()
        schemaService.getTableSchema() >> ["order_items": tableInfo]

        when: "using invalid composite key format"
        restApiService.getRecord("order_items", "invalid_format", null, null)

        then: "should throw exception for invalid format"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Composite key requires 2 parts")
    }

    def "should handle missing composite key parts"() {
        given: "a table with composite primary key"
        def tableInfo = createCompositeKeyTableInfo()
        schemaService.getTableSchema() >> ["order_items": tableInfo]

        when: "using incomplete composite key"
        restApiService.getRecord("order_items", "1", null, null)

        then: "should throw exception for incomplete key"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Composite key requires 2 parts")
    }

    def "should upsert record with primary key conflict"() {
        given: "a table with primary key"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["id": 1, "name": "John Updated", "email": "john.updated@example.com"]
        
        and: "mock successful upsert with RETURNING"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("INSERT INTO users") && 
            sql.contains("ON CONFLICT (id)") && 
            sql.contains("DO UPDATE SET") &&
            sql.contains("RETURNING *")
        }, _ as Object[]) >> [["id": 1, "name": "John Updated", "email": "john.updated@example.com"]]

        when: "upserting record"
        def result = restApiService.upsertRecord("users", data)

        then: "should return updated record"
        result.id == 1
        result.name == "John Updated"
        result.email == "john.updated@example.com"
    }

    def "should upsert record with insert when no conflict"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["name": "New User", "email": "new@example.com"]
        
        and: "mock successful insert via upsert"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 2, "name": "New User", "email": "new@example.com"]]

        when: "upserting new record"
        def result = restApiService.upsertRecord("users", data)

        then: "should return new record"
        result.id == 2
        result.name == "New User"
        result.email == "new@example.com"
    }

    def "should handle upsert with DO NOTHING when only primary keys provided"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["id": 1]
        
        and: "mock DO NOTHING result (empty)"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("ON CONFLICT (id) DO NOTHING")
        }, _ as Object[]) >> []

        when: "upserting with only primary key"
        def result = restApiService.upsertRecord("users", data)

        then: "should return null"
        result == null
    }

    def "should bulk upsert records"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def dataList = [
            ["id": 1, "name": "John Updated", "email": "john@example.com"],
            ["name": "Jane New", "email": "jane@example.com"]
        ]
        
        and: "mock successful bulk upsert"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("INSERT INTO users") && 
            sql.contains("ON CONFLICT (id)") && 
            sql.contains("VALUES") && 
            sql.count("(?, ?, ?)") == 2
        }, _ as Object[]) >> [
            ["id": 1, "name": "John Updated", "email": "john@example.com"],
            ["id": 2, "name": "Jane New", "email": "jane@example.com"]
        ]

        when: "bulk upserting records"
        def results = restApiService.upsertBulkRecords("users", dataList)

        then: "should return upserted records"
        results.size() == 2
        results[0].id == 1
        results[0].name == "John Updated"
        results[1].id == 2
        results[1].name == "Jane New"
    }

    def "should fail upsert when table has no primary key"() {
        given: "a table without primary key"
        def columns = [
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("email", "varchar", false, false)
        ]
        def tableInfo = new TableInfo("logs", columns, [])
        schemaService.getTableSchema() >> ["logs": tableInfo]
        
        def data = ["name": "Test", "email": "test@example.com"]

        when: "attempting upsert on table without primary key"
        restApiService.upsertRecord("logs", data)

        then: "should throw exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("has no primary key - cannot perform upsert")
    }

    def "should handle full-text search with fts operator"() {
        given: "a table with text column"
        def tableInfo = createPostsTableInfo()  // Use posts table with content column
        schemaService.getTableSchema() >> ["posts": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("content", "fts.postgresql tutorial")
        
        and: "mock full-text search query"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("to_tsvector('english', content)") && 
            sql.contains("plainto_tsquery('english', ?)")
        }, _ as Object[]) >> [["id": 1, "title": "PostgreSQL Guide", "content": "PostgreSQL tutorial content"]]
        
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "searching with fts operator"
        def result = restApiService.getRecords("posts", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0].title == "PostgreSQL Guide"
    }

    def "should handle phrase full-text search with plfts operator"() {
        given: "a table with text column"  
        def tableInfo = createPostsTableInfo()
        schemaService.getTableSchema() >> ["posts": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("content", "plfts.exact phrase match")
        
        and: "mock phrase search query"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("to_tsvector('english', content)") && 
            sql.contains("phraseto_tsquery('english', ?)")
        }, _ as Object[]) >> [["id": 1, "content": "This is an exact phrase match example"]]
        
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "searching with plfts operator"
        def result = restApiService.getRecords("posts", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
    }

    def "should handle websearch full-text search with wfts operator"() {
        given: "a table with text column"
        def tableInfo = createPostsTableInfo()
        schemaService.getTableSchema() >> ["posts": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("content", "wfts.postgresql OR database")
        
        and: "mock websearch query"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("to_tsvector('english', content)") && 
            sql.contains("websearch_to_tsquery('english', ?)")
        }, _ as Object[]) >> [["id": 1, "content": "PostgreSQL database tutorial"]]
        
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "searching with wfts operator"
        def result = restApiService.getRecords("posts", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
    }
    
    def "should handle bulk create records"() {
        given: "a valid table and multiple records"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def dataList = [
            ["name": "John", "email": "john@example.com"],
            ["name": "Jane", "email": "jane@example.com"]
        ]
        
        and: "mock successful inserts"
        jdbcTemplate.update(_ as String, _ as Object[]) >> 1
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "email": "john@example.com"],
            ["id": 2, "name": "Jane", "email": "jane@example.com"]
        ]

        when: "creating bulk records"
        def result = restApiService.createBulkRecords("users", dataList)

        then: "should return all created records"
        result.size() == 2
        result[0]["name"] == "John"
        result[1]["name"] == "Jane"
    }
    
    def "should handle bulk update records"() {
        given: "a valid table and update list"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def updateList = [
            ["id": "1", "data": ["name": "John Updated"]],
            ["id": "2", "data": ["name": "Jane Updated"]]
        ]
        
        and: "mock successful updates"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >>> [
            [["id": 1, "name": "John Updated", "email": "john@example.com"]],
            [["id": 2, "name": "Jane Updated", "email": "jane@example.com"]]
        ]

        when: "updating bulk records"
        def result = restApiService.updateBulkRecords("users", updateList)

        then: "should return updated records"
        result.size() == 2
        result[0]["name"] == "John Updated"
        result[1]["name"] == "Jane Updated"
    }
    
    def "should handle update records by filters"() {
        given: "a valid table with filters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def filters = new LinkedMultiValueMap<String, String>()
        filters.add("age", "gt.25")
        
        def updateData = ["name": "Updated Name"]
        
        and: "mock successful update"
        jdbcTemplate.update(_ as String, _ as Object[]) >> 2

        when: "updating records by filters"
        def result = restApiService.updateRecordsByFilters("users", filters, updateData)

        then: "should return update count"
        result["updatedCount"] == 2
    }
    
    def "should handle delete records by filters"() {
        given: "a valid table with filters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def filters = new LinkedMultiValueMap<String, String>()
        filters.add("age", "lt.18")
        
        and: "mock successful delete"
        jdbcTemplate.update(_ as String, _ as Object[]) >> 3

        when: "deleting records by filters"
        def result = restApiService.deleteRecordsByFilters("users", filters)

        then: "should return delete count"
        result["deletedCount"] == 3
    }
    
    def "should handle array operators - arraycontains"() {
        given: "a table with array columns and arraycontains filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("tags", "arraycontains.sports")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "tags": ["sports", "music"]]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with array filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle array operators - arrayhasany"() {
        given: "a table with array filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("hobbies", "arrayhasany.{reading,writing}")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "hobbies": ["reading", "cooking"]]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with arrayhasany filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle array operators - arrayhasall"() {
        given: "a table with array filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("skills", "arrayhasall.{java,spring}")
        
        and: "mock query result"  
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "skills": ["java", "spring", "postgres"]]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with arrayhasall filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle array operators - arraylength"() {
        given: "a table with array length filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("tags", "arraylength.3")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "tags": ["a", "b", "c"]]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with array length filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle JSON operators - haskey"() {
        given: "a table with JSON haskey filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("metadata", "haskey.priority")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "metadata": '{"priority": "high"}']
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with JSON haskey filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle JSON operators - haskeys"() {
        given: "a table with JSON haskeys filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("settings", 'haskeys.["theme","language"]')
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "settings": '{"theme": "dark", "language": "en"}']
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with JSON haskeys filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle JSON operators - jsoncontains"() {
        given: "a table with JSON containment filter"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("profile", 'jsoncontains.{"role":"admin"}')
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "profile": '{"role":"admin","level":5}']
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with JSON contains filter"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle string operators - startswith and endswith"() {
        given: "a table with string operator filters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("name", "startswith.John")
        params.add("email", "endswith.example.com")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John Doe", "email": "john@example.com"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with string operator filters"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John Doe"
    }
    
    def "should handle case-insensitive string operators"() {
        given: "a table with case-insensitive string filters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("name", "ilike.john%")
        params.add("email", "istartswith.JOHN")
        params.add("title", "iendswith.ADMIN")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "john smith", "email": "john@company.com", "title": "sys_admin"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with case-insensitive filters"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "john smith"
    }
    
    def "should handle is null and not null operators"() {
        given: "a table with null operator filters"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("deleted_at", "is.null")
        params.add("email", "not.null")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "email": "john@example.com", "deleted_at": null]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with null operator filters"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return matching records"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }
    
    def "should handle complex ordering with multiple columns"() {
        given: "a table with multiple column ordering"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("order", "age.desc,name.asc")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Alice", "age": 30],
            ["id": 2, "name": "Bob", "age": 30],
            ["id": 3, "name": "Charlie", "age": 25]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 3L

        when: "getting records with complex ordering"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should return properly ordered records"
        result.data.size() == 3
        result.data[0]["name"] == "Alice"
        result.data[1]["name"] == "Bob"
        result.data[2]["name"] == "Charlie"
    }
    
    def "should handle upsert operations with custom conflict columns"() {
        given: "a table with unique constraint"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["email": "john@example.com", "name": "John Updated"]
        
        and: "mock upsert result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "email": "john@example.com", "name": "John Updated"]
        ]

        when: "upserting record"
        def result = restApiService.upsertRecord("users", data)

        then: "should return upserted record"
        result["name"] == "John Updated"
        result["email"] == "john@example.com"
    }
    
    def "should validate table permissions before operations"() {
        given: "permission check returns false"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        when: "trying to access restricted table"
        restApiService.getRecords("restricted_table", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should throw security exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Table not found: restricted_table")
    }
    
    def "should handle enhanced relationship service integration"() {
        given: "a table with records"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def records = [
            ["id": 1, "name": "John", "email": "john@example.com"]
        ]
        
        and: "mock query responses"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> records
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L
        
        when: "getting basic records"
        def result = restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should return records successfully"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }

    // ===== MISSING COVERAGE TESTS =====
    
    def "should handle database constraint violations"() {
        given: "a table with constraints"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["name": "John", "email": "existing@example.com"]
        
        and: "mock constraint violation"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> { throw new DataIntegrityViolationException("Duplicate key") }

        when: "creating record with duplicate key"
        restApiService.createRecord("users", data)

        then: "should throw validation exception"
        thrown(ValidationException)
    }
    
    def "should handle SQL exceptions during operations"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock SQL exception"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> { throw new SQLException("Database error") }

        when: "creating record with SQL error"
        restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should propagate SQLException"
        thrown(SQLException)
    }

    def "should validate network addresses"() {
        given: "a table with network columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("ip_addr", "inet", false, false)
        ]
        def tableInfo = new TableInfo("network_table", columns, [])
        schemaService.getTableSchema() >> ["network_table": tableInfo]
        
        def data = ["ip_addr": "192.168.1.1"]
        
        and: "mock successful insert"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 1, "ip_addr": "192.168.1.1"]]

        when: "creating record with network address"
        def result = restApiService.createRecord("network_table", data)

        then: "should return created record with IP address"
        result.id == 1
        result.ip_addr != null
    }
    
    def "should validate MAC addresses"() {
        given: "a table with MAC address column"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("mac_addr", "macaddr", false, false)
        ]
        def tableInfo = new TableInfo("mac_table", columns, [])
        schemaService.getTableSchema() >> ["mac_table": tableInfo]
        
        def data = ["mac_addr": "08:00:2b:01:02:03"]
        
        and: "mock successful insert"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 1, "mac_addr": "08:00:2b:01:02:03"]]

        when: "creating record with MAC address"
        def result = restApiService.createRecord("mac_table", data)

        then: "should return created record with MAC address"
        result.id == 1
        result.mac_addr != null
    }

    def "should handle enum type validation"() {
        given: "a table with enum column"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("status", "status_enum", false, false)
        ]
        def tableInfo = new TableInfo("enum_table", columns, [])
        schemaService.getTableSchema() >> ["enum_table": tableInfo]
        
        def data = ["status": "active"]
        
        and: "mock successful insert"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [["id": 1, "status": "active"]]

        when: "creating record with enum value"
        def result = restApiService.createRecord("enum_table", data)

        then: "should return created record"
        result.status == "active"
    }

    def "should handle PostgreSQL arrays conversion"() {
        given: "a table with array columns"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock query with PostgreSQL arrays"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John", "tags": "{sports,music}"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with arrays"
        def result = restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should convert arrays properly"
        result.data.size() == 1
        result.data[0]["name"] == "John"
    }

    def "should handle cursor encoding and decoding"() {
        given: "a valid table"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock query with cursor pagination"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "John"],
            ["id": 2, "name": "Jane"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 2L

        when: "getting records with cursor"
        def result = restApiService.getRecordsWithCursor("users", new LinkedMultiValueMap<>(), 
                                                        "2", null, null, null, "id", "asc", null, null)

        then: "should return cursor-based results"
        result.edges.size() == 2
        result.pageInfo != null
        result.totalCount == 2
    }

    def "should handle complex column type conversion"() {
        given: "a table with various column types"
        def columns = [
            new ColumnInfo("id", "uuid", true, false),
            new ColumnInfo("timestamp_col", "timestamptz", false, false),
            new ColumnInfo("json_col", "jsonb", false, false),
            new ColumnInfo("array_col", "text[]", false, false)
        ]
        def tableInfo = new TableInfo("complex_table", columns, [])
        schemaService.getTableSchema() >> ["complex_table": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("timestamp_col", "gte.2023-01-01T00:00:00Z")
        params.add("json_col", "haskey.name")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": "123e4567-e89b-12d3-a456-426614174000", "json_col": '{"name": "test"}']
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "querying with complex types"
        def result = restApiService.getRecords("complex_table", params, 0, 10, null, "asc", null, null)

        then: "should handle complex type conversion"
        result.data.size() == 1
    }

    def "should expand single relationship"() {
        given: "a table with foreign key"
        def fkInfo = new ForeignKeyInfo("customer_id", "customers", "id")
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("customer_id", "integer", false, false),
            new ColumnInfo("order_date", "date", false, false)
        ]
        def tableInfo = new TableInfo("orders", columns, [fkInfo])
        schemaService.getTableSchema() >> ["orders": tableInfo, "customers": createSampleTableInfo()]
        
        and: "mock enhanced relationship service"
        enhancedRelationshipService.expandRelationships(_, _, _) >> []

        and: "mock query results"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "customer_id": 1, "order_date": "2023-01-01"]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with relationship expansion"
        def result = restApiService.getRecords("orders", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, "customer_id")

        then: "should expand relationship"
        result.data.size() == 1
        result.data[0]["customer_id"] == 1
    }

    def "should handle multiple relationships expansion"() {
        given: "a table with multiple foreign keys"
        def fkInfo1 = new ForeignKeyInfo("customer_id", "customers", "id")
        def fkInfo2 = new ForeignKeyInfo("product_id", "products", "id")
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("customer_id", "integer", false, false),
            new ColumnInfo("product_id", "integer", false, false)
        ]
        def tableInfo = new TableInfo("order_items", columns, [fkInfo1, fkInfo2])
        schemaService.getTableSchema() >> [
            "order_items": tableInfo, 
            "customers": createSampleTableInfo(),
            "products": createProductsTableInfo()
        ]
        
        and: "mock enhanced relationship service"
        enhancedRelationshipService.expandRelationships(_, _, _) >> []

        and: "mock query results"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "customer_id": 1, "product_id": 2]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with multiple expansions"
        def result = restApiService.getRecords("order_items", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, "customer_id,product_id")

        then: "should expand multiple relationships"
        result.data.size() == 1
        result.data[0]["customer_id"] == 1
        result.data[0]["product_id"] == 2
    }

    def "should handle invalid table name characters"() {
        when: "using table name with invalid characters"
        restApiService.getRecords("users'; DROP TABLE users; --", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should throw validation exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Invalid table name")
    }

    def "should handle empty table name"() {
        when: "using empty table name"
        restApiService.getRecords("", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should throw validation exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Table name cannot be empty")
    }

    def "should handle permission check failures gracefully"() {
        given: "a table that exists"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        and: "mock permission check failure"
        jdbcTemplate.queryForObject("SELECT has_table_privilege(current_user, ?, ?)", Boolean.class, _, _) >> false

        when: "getting records without permission"
        restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null)

        then: "should allow access (fallback behavior)"
        1 * jdbcTemplate.queryForList(_ as String, _ as Object[]) >> []
        1 * jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 0L
    }

    def "should validate delete with empty filters"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]

        when: "deleting with empty filters"
        restApiService.deleteRecordsByFilters("users", new LinkedMultiValueMap<>())

        then: "should throw validation exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Filters cannot be empty")
    }

    def "should validate update with empty filters"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]

        when: "updating with empty filters"
        restApiService.updateRecordsByFilters("users", new LinkedMultiValueMap<>(), ["name": "test"])

        then: "should throw validation exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Filters cannot be empty")
    }

    def "should validate update with empty data"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def filters = new LinkedMultiValueMap<String, String>()
        filters.add("id", "eq.1")

        when: "updating with empty data"
        restApiService.updateRecordsByFilters("users", filters, [:])

        then: "should throw validation exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Update data cannot be empty")
    }

    def "should handle order by parsing with multiple columns"() {
        given: "a table with multiple columns"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("order", "name.asc,age.desc")
        
        and: "mock query result"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Alice", "age": 30]
        ]
        jdbcTemplate.queryForObject(_ as String, Long.class, _ as Object[]) >> 1L

        when: "getting records with complex ordering"
        def result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should apply complex ordering"
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("ORDER BY") && sql.contains("name") && sql.contains("age")
        }, _ as Object[]) >> [["id": 1, "name": "Alice", "age": 30]]
        result.data != null
        result.data.size() == 1
    }

    def "should handle invalid column in order by"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def params = new LinkedMultiValueMap<String, String>()
        params.add("order", "invalid_column.asc")

        when: "using invalid column in order by"
        restApiService.getRecords("users", params, 0, 10, null, "asc", null, null)

        then: "should throw validation exception"
        def exception = thrown(IllegalArgumentException)
        exception.message.contains("Invalid column")
    }

    def "should handle database constraint violation with message extraction"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["name": "Test", "email": "test@example.com"]
        
        and: "mock constraint violation with specific message"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> { 
            throw new DataIntegrityViolationException("ERROR: duplicate key value violates unique constraint \"users_email_key\"")
        }

        when: "creating record with constraint violation"
        restApiService.createRecord("users", data)

        then: "should throw validation exception"
        thrown(ValidationException)
    }
    
    def "should handle SQL constraint violations during update"() {
        given: "a table info"
        def tableInfo = createSampleTableInfo()
        schemaService.getTableSchema() >> ["users": tableInfo]
        
        def data = ["name": "Updated Name"]
        
        and: "mock SQL constraint violation"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> { 
            throw new DataIntegrityViolationException("Foreign key constraint violation")
        }

        when: "updating record with constraint violation"
        restApiService.updateRecord("users", "1", data, false)

        then: "should propagate validation exception"
        thrown(ValidationException)
    }

    private TableInfo createSampleTableInfo() {
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("email", "varchar", false, false),
            new ColumnInfo("age", "integer", false, true),
            new ColumnInfo("tags", "text[]", false, true),
            new ColumnInfo("hobbies", "text[]", false, true), 
            new ColumnInfo("skills", "text[]", false, true),
            new ColumnInfo("metadata", "jsonb", false, true),
            new ColumnInfo("settings", "jsonb", false, true),
            new ColumnInfo("profile", "jsonb", false, true),
            new ColumnInfo("title", "varchar", false, true),
            new ColumnInfo("deleted_at", "timestamp", false, true)
        ]
        return new TableInfo("users", columns, [])
    }
    
    private TableInfo createCompositeKeyTableInfo() {
        def columns = [
            new ColumnInfo("order_id", "integer", true, false),
            new ColumnInfo("product_id", "integer", true, false),
            new ColumnInfo("quantity", "integer", false, false),
            new ColumnInfo("price", "decimal", false, false)
        ]
        return new TableInfo("order_items", columns, [])
    }
    
    private TableInfo createPostsTableInfo() {
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("title", "varchar", false, false),
            new ColumnInfo("content", "text", false, false),
            new ColumnInfo("created_at", "timestamp", false, false)
        ]
        return new TableInfo("posts", columns, [])
    }
    
    private TableInfo createProductsTableInfo() {
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("price", "decimal", false, false),
            new ColumnInfo("is_active", "boolean", false, false)
        ]
        return new TableInfo("products", columns, [])
    }

    // ===== ARRAY HANDLING TESTS =====
    
    def "should create record with array data"() {
        given: "a table with array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("tags", "text[]", false, false),
            new ColumnInfo("numbers", "integer[]", false, false)
        ]
        def tableInfo = new TableInfo("array_table", columns, [])
        schemaService.getTableSchema() >> ["array_table": tableInfo]
        
        def data = [
            "name": "Test User",
            "tags": ["sports", "music", "travel"],
            "numbers": [1, 2, 3, 4, 5]
        ]
        
        and: "mock successful insert with array data"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Test User", "tags": ["sports", "music", "travel"], "numbers": [1, 2, 3, 4, 5]]
        ]

        when: "creating record with array data"
        def result = restApiService.createRecord("array_table", data)

        then: "should return created record with array data"
        result.id == 1
        result.name == "Test User"
        result.tags == ["sports", "music", "travel"]
        result.numbers == [1, 2, 3, 4, 5]
    }
    
    def "should update record with array data"() {
        given: "a table with array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("tags", "text[]", false, false),
            new ColumnInfo("scores", "numeric[]", false, false)
        ]
        def tableInfo = new TableInfo("array_table", columns, [])
        schemaService.getTableSchema() >> ["array_table": tableInfo]
        
        def updateData = [
            "tags": ["updated", "array", "values"],
            "scores": [9.5, 8.7, 10.0]
        ]
        
        and: "mock successful update with array data"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Test User", "tags": ["updated", "array", "values"], "scores": [9.5, 8.7, 10.0]]
        ]

        when: "updating record with array data"
        def result = restApiService.updateRecord("array_table", "1", updateData, false)

        then: "should return updated record with array data"
        result.id == 1
        result.tags == ["updated", "array", "values"]
        result.scores == [9.5, 8.7, 10.0]
    }
    
    def "should create record with empty arrays"() {
        given: "a table with array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("tags", "text[]", false, false),
            new ColumnInfo("values", "integer[]", false, false)
        ]
        def tableInfo = new TableInfo("array_table", columns, [])
        schemaService.getTableSchema() >> ["array_table": tableInfo]
        
        def data = [
            "name": "Empty Arrays User",
            "tags": [],
            "values": []
        ]
        
        and: "mock successful insert with empty arrays"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Empty Arrays User", "tags": [], "values": []]
        ]

        when: "creating record with empty arrays"
        def result = restApiService.createRecord("array_table", data)

        then: "should return created record with empty arrays"
        result.id == 1
        result.name == "Empty Arrays User"
        result.tags == []
        result.values == []
    }
    
    def "should update record with null arrays"() {
        given: "a table with nullable array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("tags", "text[]", false, true),
            new ColumnInfo("scores", "numeric[]", false, true)
        ]
        def tableInfo = new TableInfo("array_table", columns, [])
        schemaService.getTableSchema() >> ["array_table": tableInfo]
        
        def updateData = [
            "tags": null,
            "scores": null
        ]
        
        and: "mock successful update with null arrays"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Test User", "tags": null, "scores": null]
        ]

        when: "updating record with null arrays"
        def result = restApiService.updateRecord("array_table", "1", updateData, false)

        then: "should return updated record with null arrays"
        result.id == 1
        result.tags == null
        result.scores == null
    }
    
    def "should create bulk records with array data"() {
        given: "a table with array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("categories", "text[]", false, false)
        ]
        def tableInfo = new TableInfo("array_table", columns, [])
        schemaService.getTableSchema() >> ["array_table": tableInfo]
        
        def dataList = [
            ["name": "User 1", "categories": ["tech", "science"]],
            ["name": "User 2", "categories": ["art", "music", "literature"]],
            ["name": "User 3", "categories": ["sports"]]
        ]
        
        and: "mock successful bulk insert with array data"
        jdbcTemplate.update(_ as String, _ as Object[]) >> 1
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "User 1", "categories": ["tech", "science"]],
            ["id": 2, "name": "User 2", "categories": ["art", "music", "literature"]],
            ["id": 3, "name": "User 3", "categories": ["sports"]]
        ]

        when: "creating bulk records with array data"
        def result = restApiService.createBulkRecords("array_table", dataList)

        then: "should return all created records with array data"
        result.size() == 3
        result[0]["name"] == "User 1"
        result[0]["categories"] == ["tech", "science"]
        result[1]["name"] == "User 2"
        result[1]["categories"] == ["art", "music", "literature"]
        result[2]["name"] == "User 3"
        result[2]["categories"] == ["sports"]
    }
    
    def "should upsert record with array data"() {
        given: "a table with array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("skills", "text[]", false, false)
        ]
        def tableInfo = new TableInfo("array_table", columns, [])
        schemaService.getTableSchema() >> ["array_table": tableInfo]
        
        def data = [
            "id": 1,
            "name": "Updated User",
            "skills": ["java", "spring", "postgresql", "arrays"]
        ]
        
        and: "mock successful upsert with array data"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("INSERT INTO array_table") &&
            sql.contains("ON CONFLICT (id)") &&
            sql.contains("DO UPDATE SET")
        }, _ as Object[]) >> [
            ["id": 1, "name": "Updated User", "skills": ["java", "spring", "postgresql", "arrays"]]
        ]

        when: "upserting record with array data"
        def result = restApiService.upsertRecord("array_table", data)

        then: "should return upserted record with array data"
        result.id == 1
        result.name == "Updated User"
        result.skills == ["java", "spring", "postgresql", "arrays"]
    }
    
    def "should handle multidimensional arrays"() {
        given: "a table with multidimensional array column"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("matrix", "integer[][]", false, false)
        ]
        def tableInfo = new TableInfo("matrix_table", columns, [])
        schemaService.getTableSchema() >> ["matrix_table": tableInfo]
        
        def data = [
            "name": "Matrix User",
            "matrix": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        ]
        
        and: "mock successful insert with multidimensional array"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Matrix User", "matrix": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]]
        ]

        when: "creating record with multidimensional array"
        def result = restApiService.createRecord("matrix_table", data)

        then: "should return created record with multidimensional array"
        result.id == 1
        result.name == "Matrix User"
        result.matrix == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    }
    
    def "should handle mixed type arrays with proper conversion"() {
        given: "a table with various array types"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("text_array", "text[]", false, false),
            new ColumnInfo("int_array", "integer[]", false, false),
            new ColumnInfo("bool_array", "boolean[]", false, false),
            new ColumnInfo("decimal_array", "decimal[]", false, false)
        ]
        def tableInfo = new TableInfo("mixed_arrays", columns, [])
        schemaService.getTableSchema() >> ["mixed_arrays": tableInfo]
        
        def data = [
            "text_array": ["hello", "world", "test"],
            "int_array": [10, 20, 30],
            "bool_array": [true, false, true],
            "decimal_array": [1.1, 2.2, 3.3]
        ]
        
        and: "mock successful insert with mixed arrays"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "text_array": ["hello", "world", "test"], "int_array": [10, 20, 30], 
             "bool_array": [true, false, true], "decimal_array": [1.1, 2.2, 3.3]]
        ]

        when: "creating record with mixed array types"
        def result = restApiService.createRecord("mixed_arrays", data)

        then: "should return created record with properly converted arrays"
        result.id == 1
        result.text_array == ["hello", "world", "test"]
        result.int_array == [10, 20, 30]
        result.bool_array == [true, false, true]
        result.decimal_array == [1.1, 2.2, 3.3]
    }
    
    // ===== RELATIONSHIP EXPANSION TESTS =====
    
    def "should expand reverse relationship successfully"() {
        given: "tables with reverse relationship"
        def userColumns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false)
        ]
        def orderColumns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("user_id", "integer", false, false),
            new ColumnInfo("total", "decimal", false, false)
        ]
        def userTableInfo = new TableInfo("users", userColumns, [])
        def orderTableInfo = new TableInfo("orders", orderColumns, [
            new ForeignKeyInfo("user_id", "users", "id")
        ])
        
        def users = [
            ["id": 1, "name": "User 1"],
            ["id": 2, "name": "User 2"]
        ]
        def fk = new ForeignKeyInfo("user_id", "users", "id")
        def expansionParams = ["select": "*", "limit": "5"]
        
        and: "mock batch loader returns related records"
        def mockOrderRecords = [
            1: [["id": 101, "user_id": 1, "total": 50.00], ["id": 102, "user_id": 1, "total": 75.00]],
            2: [["id": 103, "user_id": 2, "total": 25.00]]
        ]
        batchLoader.loadRelatedRecords("orders", "user_id", "id", [1, 2] as Set, "*", 5) >> mockOrderRecords

        when: "expanding reverse relationship"
        restApiService.expandReverseRelationship(users, userTableInfo, "orders", fk, expansionParams)

        then: "users should have orders expanded"
        users[0]["orders"] == [["id": 101, "user_id": 1, "total": 50.00], ["id": 102, "user_id": 1, "total": 75.00]]
        users[1]["orders"] == [["id": 103, "user_id": 2, "total": 25.00]]
    }
    
    def "should handle empty reverse relationship"() {
        given: "tables with reverse relationship but no related records"
        def userColumns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false)
        ]
        def userTableInfo = new TableInfo("users", userColumns, [])
        
        def users = [["id": 1, "name": "User 1"]]
        def fk = new ForeignKeyInfo("user_id", "users", "id")
        def expansionParams = ["select": "*"]
        
        and: "batch loader returns empty results"
        batchLoader.loadRelatedRecords("orders", "user_id", "id", [1] as Set, "*", 0) >> [:]

        when: "expanding reverse relationship with no related records"
        restApiService.expandReverseRelationship(users, userTableInfo, "orders", fk, expansionParams)

        then: "users should have empty arrays for orders"
        users[0]["orders"] == []
    }

    // ===== VALIDATION METHOD TESTS =====
    
    def "should validate enum values successfully"() {
        given: "mock schema service with enum values"
        schemaService.getEnumValues("status_enum") >> ["active", "inactive", "pending"]

        when: "validating valid enum value"
        def result = restApiService.validateEnumValue("status_enum", "active")

        then: "should return the value"
        result == "active"
    }
    
    def "should return value as fallback for invalid enum value"() {
        given: "mock schema service with enum values"
        schemaService.getEnumValues("status_enum") >> ["active", "inactive"]

        when: "validating invalid enum value"
        def result = restApiService.validateEnumValue("status_enum", "invalid_status")

        then: "should return the value as fallback"
        result == "invalid_status"
    }
    
    def "should validate network addresses successfully"() {
        when: "validating valid IP addresses"
        def ipv4Result = restApiService.validateNetworkAddress("192.168.1.1")
        def ipv6Result = restApiService.validateNetworkAddress("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
        def cidrResult = restApiService.validateNetworkAddress("192.168.0.0/24")

        then: "should return the addresses"
        ipv4Result == "192.168.1.1"
        ipv6Result == "2001:0db8:85a3:0000:0000:8a2e:0370:7334" 
        cidrResult == "192.168.0.0/24"
    }
    
    def "should throw exception for invalid network address"() {
        when: "validating invalid network address"
        restApiService.validateNetworkAddress("invalid.ip.address")

        then: "should throw illegal argument exception"
        thrown(IllegalArgumentException)
    }
    
    def "should validate MAC addresses successfully"() {
        when: "validating valid MAC addresses"
        def mac1 = restApiService.validateMacAddress("00:1B:44:11:3A:B7")
        def mac2 = restApiService.validateMacAddress("00-1B-44-11-3A-B7")
        def mac8 = restApiService.validateMacAddress("00:1B:44:11:3A:B7:C8:D9") // MACADDR8 format

        then: "should return the addresses"
        mac1 == "00:1B:44:11:3A:B7"
        mac2 == "00-1B-44-11-3A-B7"
        mac8 == "00:1B:44:11:3A:B7:C8:D9"
    }
    
    def "should throw exception for invalid MAC address"() {
        when: "validating invalid MAC address"
        restApiService.validateMacAddress("invalid-mac-address")

        then: "should throw illegal argument exception"
        thrown(IllegalArgumentException)
    }

    // ===== ERROR HANDLING TESTS =====
    
    def "should extract enum type from constraint message"() {
        given: "constraint violation message with enum type"
        def message = 'invalid input value for enum status_enum: "invalid_value"'

        when: "extracting enum type"
        def result = restApiService.extractEnumTypeFromMessage(message)

        then: "should return enum type name"
        result == "status_enum"
    }
    
    def "should return unknown for message without enum type"() {
        given: "constraint violation message without enum type"
        def message = "some other constraint violation message"

        when: "extracting enum type"
        def result = restApiService.extractEnumTypeFromMessage(message)

        then: "should return unknown"
        result == "unknown"
    }
    
    def "should extract column name from constraint message"() {
        given: "constraint violation messages with column names"
        def notNullMessage = 'null value in column "email" violates not-null constraint'
        def uniqueMessage = 'duplicate key value in column "username" violates unique constraint'

        when: "extracting column names"
        def notNullResult = restApiService.extractColumnNameFromConstraint(notNullMessage, "violates not-null constraint")
        def uniqueResult = restApiService.extractColumnNameFromConstraint(uniqueMessage, "violates unique constraint")

        then: "should return column names"
        notNullResult == "email"
        uniqueResult == "username"
    }
    
    def "should handle SQL constraint violations"() {
        given: "SQL exception with constraint violation"
        def sqlException = new SQLException("ERROR: duplicate key value violates unique constraint \"users_email_key\"\n  Detail: Key (email)=(test@example.com) already exists.")
        def data = ["email": "test@example.com", "name": "Test User"]

        when: "handling SQL constraint violation"
        restApiService.handleSqlConstraintViolation(sqlException, "users", data)

        then: "should throw validation exception with proper message"
        def ex = thrown(ValidationException)
        ex.message.contains("email")
        ex.message.contains("test@example.com")
    }
    
    def "should handle SQL constraint violations with enum types"() {
        given: "SQL exception with enum constraint violation"
        def sqlException = new SQLException('ERROR: invalid input value for enum status_enum: "invalid_status"\n  Detail: Valid values are: active, inactive, pending.')
        def data = ["status": "invalid_status", "name": "Test User"]

        when: "handling SQL constraint violation with enum"
        restApiService.handleSqlConstraintViolation(sqlException, "users", data)

        then: "should throw validation exception with enum type"
        def ex = thrown(ValidationException)
        ex.message.contains("status_enum")
    }
    
    // ===== JSON/JSONB CREATE/UPDATE TESTS =====
    
    def "should create record with JSONB data"() {
        given: "a table with JSONB columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("metadata", "jsonb", false, false),
            new ColumnInfo("settings", "jsonb", false, false)
        ]
        def tableInfo = new TableInfo("jsonb_table", columns, [])
        schemaService.getTableSchema() >> ["jsonb_table": tableInfo]
        
        def data = [
            "name": "Test User",
            "metadata": [
                "profile": [
                    "age": 30,
                    "city": "New York",
                    "preferences": ["coffee", "books", "travel"]
                ],
                "tags": ["developer", "tech"]
            ],
            "settings": [
                "theme": "dark",
                "notifications": [
                    "email": true,
                    "push": false,
                    "sms": true
                ]
            ]
        ]
        
        and: "mock successful insert with JSONB data"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Test User", "metadata": data.metadata, "settings": data.settings]
        ]

        when: "creating record with JSONB data"
        def result = restApiService.createRecord("jsonb_table", data)

        then: "should return created record with JSONB data"
        result.id == 1
        result.name == "Test User"
        result.metadata != null
        result.settings != null
    }
    
    def "should update record with JSONB data"() {
        given: "a table with JSONB columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("profile", "jsonb", false, false),
            new ColumnInfo("config", "jsonb", false, false)
        ]
        def tableInfo = new TableInfo("jsonb_table", columns, [])
        schemaService.getTableSchema() >> ["jsonb_table": tableInfo]
        
        def updateData = [
            "profile": [
                "bio": "Updated bio content",
                "skills": ["Java", "Spring Boot", "PostgreSQL", "JSONB"],
                "experience": [
                    "years": 5,
                    "companies": ["TechCorp", "DevStudio", "CodeFactory"]
                ],
                "contact": [
                    "email": "updated@example.com",
                    "social": [
                        "twitter": "@updated_user",
                        "linkedin": "linkedin.com/in/updated"
                    ]
                ]
            ],
            "config": [
                "dashboard": [
                    "layout": "grid",
                    "widgets": ["chart", "table", "metrics"],
                    "refresh_rate": 30
                ],
                "api": [
                    "rate_limit": 1000,
                    "timeout": 30000
                ]
            ]
        ]
        
        and: "mock successful update with JSONB data"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Test User", "profile": updateData.profile, "config": updateData.config]
        ]

        when: "updating record with JSONB data"
        def result = restApiService.updateRecord("jsonb_table", "1", updateData, false)

        then: "should return updated record with JSONB data"
        result.id == 1
        result.profile != null
        result.config != null
    }
    
    def "should create record with JSON data"() {
        given: "a table with JSON columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("data", "json", false, false),
            new ColumnInfo("properties", "json", false, false)
        ]
        def tableInfo = new TableInfo("json_table", columns, [])
        schemaService.getTableSchema() >> ["json_table": tableInfo]
        
        def data = [
            "name": "JSON Test",
            "data": [
                "type": "user",
                "status": "active",
                "attributes": [
                    "verified": true,
                    "premium": false,
                    "created": "2023-01-01T10:00:00Z"
                ]
            ],
            "properties": [
                "permissions": ["read", "write", "delete"],
                "limits": [
                    "daily_requests": 10000,
                    "concurrent_connections": 50
                ]
            ]
        ]
        
        and: "mock successful insert with JSON data"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "JSON Test", "data": data.data, "properties": data.properties]
        ]

        when: "creating record with JSON data"
        def result = restApiService.createRecord("json_table", data)

        then: "should return created record with JSON data"
        result.id == 1
        result.name == "JSON Test"
        result.data != null
        result.properties != null
    }
    
    def "should update record with JSON data"() {
        given: "a table with JSON columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("document", "json", false, false),
            new ColumnInfo("metadata", "json", false, false)
        ]
        def tableInfo = new TableInfo("json_table", columns, [])
        schemaService.getTableSchema() >> ["json_table": tableInfo]
        
        def updateData = [
            "document": [
                "title": "Updated Document",
                "content": [
                    "sections": [
                        [
                            "heading": "Introduction",
                            "paragraphs": ["First paragraph", "Second paragraph"]
                        ],
                        [
                            "heading": "Conclusion", 
                            "paragraphs": ["Final thoughts"]
                        ]
                    ]
                ],
                "version": 2.1,
                "last_modified": "2023-12-01T15:30:00Z"
            ],
            "metadata": [
                "author": "John Doe",
                "tags": ["documentation", "update", "json"],
                "statistics": [
                    "word_count": 1250,
                    "read_time_minutes": 5
                ]
            ]
        ]
        
        and: "mock successful update with JSON data"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "document": updateData.document, "metadata": updateData.metadata]
        ]

        when: "updating record with JSON data"
        def result = restApiService.updateRecord("json_table", "1", updateData, false)

        then: "should return updated record with JSON data"
        result.id == 1
        result.document != null
        result.metadata != null
    }
    
    // ===== ARRAY CREATE/UPDATE TESTS =====
    
    def "should create record with various PostgreSQL array types"() {
        given: "a table with multiple array column types"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("text_array", "text[]", false, false),
            new ColumnInfo("integer_array", "integer[]", false, false),
            new ColumnInfo("decimal_array", "decimal[]", false, false),
            new ColumnInfo("boolean_array", "boolean[]", false, false),
            new ColumnInfo("varchar_array", "varchar[]", false, false),
            new ColumnInfo("uuid_array", "uuid[]", false, false),
            new ColumnInfo("timestamp_array", "timestamp[]", false, false)
        ]
        def tableInfo = new TableInfo("array_types", columns, [])
        schemaService.getTableSchema() >> ["array_types": tableInfo]
        
        def data = [
            "text_array": ["hello", "world", "test", "postgresql"],
            "integer_array": [1, 2, 3, 100, -50],
            "decimal_array": [1.1, 2.5, 99.99, -10.25],
            "boolean_array": [true, false, true, true],
            "varchar_array": ["short", "medium length", "very long string content"],
            "uuid_array": ["123e4567-e89b-12d3-a456-426614174000", "987fcdeb-51a2-43d1-b789-123456789abc"],
            "timestamp_array": ["2023-01-01T10:00:00", "2023-12-31T23:59:59"]
        ]
        
        and: "mock successful insert with array types"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1] + data
        ]

        when: "creating record with various array types"
        def result = restApiService.createRecord("array_types", data)

        then: "should return created record with array data"
        result.id == 1
        result.text_array != null
        result.integer_array != null
        result.decimal_array != null
        result.boolean_array != null
        result.varchar_array != null
        result.uuid_array != null
        result.timestamp_array != null
    }
    
    def "should update record with PostgreSQL arrays"() {
        given: "a table with array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("tags", "text[]", false, false),
            new ColumnInfo("scores", "numeric[]", false, false),
            new ColumnInfo("flags", "boolean[]", false, false),
            new ColumnInfo("categories", "varchar[]", false, false)
        ]
        def tableInfo = new TableInfo("array_updates", columns, [])
        schemaService.getTableSchema() >> ["array_updates": tableInfo]
        
        def updateData = [
            "tags": ["updated", "new-feature", "testing", "arrays", "postgresql"],
            "scores": [95.5, 87.2, 100.0, 92.8, 88.1],
            "flags": [true, true, false, true, false, true],
            "categories": ["tech", "database", "development", "backend"]
        ]
        
        and: "mock successful update with arrays"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1] + updateData
        ]

        when: "updating record with array data"
        def result = restApiService.updateRecord("array_updates", "1", updateData, false)

        then: "should return updated record with array data"
        result.id == 1
        result.tags != null
        result.scores != null
        result.flags != null
        result.categories != null
    }
    
    def "should create record with nested array structures"() {
        given: "a table with multidimensional arrays"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("matrix", "integer[][]", false, false),
            new ColumnInfo("nested_text", "text[][]", false, false)
        ]
        def tableInfo = new TableInfo("nested_arrays", columns, [])
        schemaService.getTableSchema() >> ["nested_arrays": tableInfo]
        
        def data = [
            "name": "Nested Array Test",
            "matrix": [
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9]
            ],
            "nested_text": [
                ["hello", "world"],
                ["foo", "bar", "baz"],
                ["postgresql", "arrays"]
            ]
        ]
        
        and: "mock successful insert with nested arrays"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Nested Array Test", "matrix": data.matrix, "nested_text": data.nested_text]
        ]

        when: "creating record with nested arrays"
        def result = restApiService.createRecord("nested_arrays", data)

        then: "should return created record with nested array data"
        result.id == 1
        result.name == "Nested Array Test"
        result.matrix != null
        result.nested_text != null
    }
    
    // ===== COMPLEX JSON + ARRAY COMBINATIONS =====
    
    def "should create record with complex JSON containing arrays"() {
        given: "a table with complex JSONB structure containing arrays"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("complex_data", "jsonb", false, false)
        ]
        def tableInfo = new TableInfo("complex_json", columns, [])
        schemaService.getTableSchema() >> ["complex_json": tableInfo]
        
        def data = [
            "name": "Complex JSON Test",
            "complex_data": [
                "user_profile": [
                    "basic_info": [
                        "name": "John Doe",
                        "age": 30,
                        "emails": ["john@work.com", "john@personal.com"],
                        "phone_numbers": ["+1-555-0123", "+1-555-0456"]
                    ],
                    "preferences": [
                        "languages": ["English", "Spanish", "French"],
                        "hobbies": ["reading", "coding", "hiking"],
                        "skills": [
                            ["Java", "Expert"],
                            ["PostgreSQL", "Advanced"],
                            ["Spring", "Intermediate"]
                        ]
                    ]
                ],
                "activity_log": [
                    [
                        "date": "2023-12-01",
                        "actions": ["login", "view_dashboard", "update_profile"],
                        "duration_minutes": 45
                    ],
                    [
                        "date": "2023-12-02", 
                        "actions": ["login", "create_report", "logout"],
                        "duration_minutes": 120
                    ]
                ],
                "settings": [
                    "notifications": [
                        "email": ["security", "updates"],
                        "push": ["messages", "alerts"],
                        "sms": []
                    ],
                    "privacy": [
                        "visible_fields": ["name", "email"],
                        "hidden_fields": ["phone", "address"]
                    ]
                ]
            ]
        ]
        
        and: "mock successful insert with complex JSON"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "Complex JSON Test", "complex_data": data.complex_data]
        ]

        when: "creating record with complex JSON containing arrays"
        def result = restApiService.createRecord("complex_json", data)

        then: "should return created record with complex JSON data"
        result.id == 1
        result.name == "Complex JSON Test"
        result.complex_data != null
    }
    
    def "should update record with mixed JSON and array columns"() {
        given: "a table with both JSON and array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("json_config", "jsonb", false, false),
            new ColumnInfo("tag_array", "text[]", false, false),
            new ColumnInfo("number_array", "integer[]", false, false),
            new ColumnInfo("json_data", "json", false, false)
        ]
        def tableInfo = new TableInfo("mixed_types", columns, [])
        schemaService.getTableSchema() >> ["mixed_types": tableInfo]
        
        def updateData = [
            "json_config": [
                "api_settings": [
                    "endpoints": [
                        "base_url": "https://api.example.com",
                        "version": "v1",
                        "timeout": 30000
                    ],
                    "authentication": [
                        "type": "bearer",
                        "scopes": ["read", "write", "admin"]
                    ]
                ],
                "feature_flags": [
                    "new_dashboard": true,
                    "beta_features": false,
                    "analytics": true
                ]
            ],
            "tag_array": ["api", "configuration", "update", "production"],
            "number_array": [200, 201, 400, 401, 403, 404, 500],
            "json_data": [
                "metrics": [
                    "requests_per_day": 50000,
                    "average_response_time": 150,
                    "error_rate": 0.02
                ],
                "deployment": [
                    "environment": "production",
                    "version": "1.2.3",
                    "deployed_at": "2023-12-01T10:00:00Z",
                    "servers": ["web-01", "web-02", "web-03"]
                ]
            ]
        ]
        
        and: "mock successful update with mixed types"
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1] + updateData
        ]

        when: "updating record with mixed JSON and array data"
        def result = restApiService.updateRecord("mixed_types", "1", updateData, false)

        then: "should return updated record with mixed type data"
        result.id == 1
        result.json_config != null
        result.tag_array != null
        result.number_array != null
        result.json_data != null
    }
    
    // ===== BULK OPERATIONS WITH COMPLEX TYPES =====
    
    def "should bulk create records with JSON and array data"() {
        given: "a table with JSON and array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("metadata", "jsonb", false, false),
            new ColumnInfo("tags", "text[]", false, false)
        ]
        def tableInfo = new TableInfo("bulk_complex", columns, [])
        schemaService.getTableSchema() >> ["bulk_complex": tableInfo]
        
        def dataList = [
            [
                "name": "User 1",
                "metadata": [
                    "role": "admin",
                    "permissions": ["read", "write", "delete"],
                    "profile": ["department": "engineering", "level": "senior"]
                ],
                "tags": ["admin", "engineering", "senior"]
            ],
            [
                "name": "User 2", 
                "metadata": [
                    "role": "user",
                    "permissions": ["read"],
                    "profile": ["department": "marketing", "level": "junior"]
                ],
                "tags": ["user", "marketing", "junior"]
            ],
            [
                "name": "User 3",
                "metadata": [
                    "role": "moderator",
                    "permissions": ["read", "write"],
                    "profile": ["department": "support", "level": "mid"]
                ],
                "tags": ["moderator", "support", "mid-level"]
            ]
        ]
        
        and: "mock successful bulk insert"
        jdbcTemplate.update(_ as String, _ as Object[]) >> 1
        jdbcTemplate.queryForList(_ as String, _ as Object[]) >> [
            ["id": 1, "name": "User 1", "metadata": dataList[0].metadata, "tags": dataList[0].tags],
            ["id": 2, "name": "User 2", "metadata": dataList[1].metadata, "tags": dataList[1].tags], 
            ["id": 3, "name": "User 3", "metadata": dataList[2].metadata, "tags": dataList[2].tags]
        ]

        when: "bulk creating records with complex data"
        def result = restApiService.createBulkRecords("bulk_complex", dataList)

        then: "should return all created records with complex data"
        result.size() == 3
        result.every { it.metadata != null }
        result.every { it.tags != null }
    }
    
    def "should upsert record with JSON and array data"() {
        given: "a table with JSON and array columns"
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("config", "jsonb", false, false),
            new ColumnInfo("categories", "text[]", false, false)
        ]
        def tableInfo = new TableInfo("upsert_complex", columns, [])
        schemaService.getTableSchema() >> ["upsert_complex": tableInfo]
        
        def data = [
            "id": 1,
            "name": "Upsert Test",
            "config": [
                "settings": [
                    "theme": "dark",
                    "language": "en",
                    "timezone": "UTC"
                ],
                "preferences": [
                    "notifications": true,
                    "auto_save": false,
                    "sync_enabled": true
                ],
                "custom_fields": [
                    ["field_name": "priority", "field_type": "select", "options": ["low", "medium", "high"]],
                    ["field_name": "department", "field_type": "text", "required": true]
                ]
            ],
            "categories": ["configuration", "user-preference", "custom-fields", "upsert-test"]
        ]
        
        and: "mock successful upsert"
        jdbcTemplate.queryForList({ String sql ->
            sql.contains("INSERT INTO upsert_complex") &&
            sql.contains("ON CONFLICT (id)") &&
            sql.contains("DO UPDATE SET")
        }, _ as Object[]) >> [
            ["id": 1, "name": "Upsert Test", "config": data.config, "categories": data.categories]
        ]

        when: "upserting record with complex data"
        def result = restApiService.upsertRecord("upsert_complex", data)

        then: "should return upserted record with complex data"
        result.id == 1
        result.name == "Upsert Test"
        result.config != null
        result.categories != null
    }
}