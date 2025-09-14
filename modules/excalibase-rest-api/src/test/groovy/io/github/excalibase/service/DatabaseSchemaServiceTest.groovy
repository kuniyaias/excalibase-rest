package io.github.excalibase.service

import io.github.excalibase.constant.DatabaseType
import io.github.excalibase.model.ColumnInfo
import io.github.excalibase.model.TableInfo
import io.github.excalibase.postgres.service.DatabaseSchemaService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import spock.lang.Specification
import spock.lang.Subject

class DatabaseSchemaServiceTest extends Specification {

    def jdbcTemplate = Mock(JdbcTemplate)
    
    @Subject
    DatabaseSchemaService schemaService = new DatabaseSchemaService(jdbcTemplate, "test_schema", "postgres")

    def "should return cached schema when cache is valid"() {
        given: "a schema is already cached"
        def tableInfo = new TableInfo("test_table", [], [])
        schemaService.schemaCache.put("test_schema", ["test_table": tableInfo])
        schemaService.lastCacheUpdate = System.currentTimeMillis()

        when: "getting table schema"
        def result = schemaService.getTableSchema()

        then: "should return cached schema without database calls"
        result.size() == 1
        result["test_table"] == tableInfo
        0 * jdbcTemplate._
    }

    def "should refresh cache when TTL expired"() {
        given: "an expired cache"
        schemaService.schemaCache.put("test_schema", ["old_table": new TableInfo()])
        schemaService.lastCacheUpdate = System.currentTimeMillis() - 400_000 // 6+ minutes ago

        and: "mock database responses"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["new_table"]
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "new_table", "test_schema", "new_table") >> 
            [new ColumnInfo("id", "integer", true, false)]
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "new_table") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "new_table") >> 0

        when: "getting table schema"
        def result = schemaService.getTableSchema()

        then: "should refresh cache with new data"
        result.size() == 1
        result.containsKey("new_table")
        !result.containsKey("old_table")
    }

    def "should handle database connection errors gracefully"() {
        given: "database throws exception"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> { throw new RuntimeException("DB connection failed") }

        when: "getting table schema"
        schemaService.getTableSchema()

        then: "should propagate exception with context"
        def exception = thrown(RuntimeException)
        exception.message == "Failed to reflect database schema"
        exception.cause.message == "DB connection failed"
    }

    def "should clear cache when requested"() {
        given: "a populated cache"
        schemaService.schemaCache.put("test_schema", ["table1": new TableInfo()])
        schemaService.lastCacheUpdate = System.currentTimeMillis()

        when: "clearing cache"
        schemaService.clearCache()

        then: "cache should be empty"
        schemaService.schemaCache.isEmpty()
        schemaService.lastCacheUpdate == 0
    }

    def "should return correct allowed schema and database type"() {
        expect: "configuration values are returned correctly"
        schemaService.allowedSchema == "test_schema"
        schemaService.databaseType == DatabaseType.POSTGRES
    }

    def "should build table info with columns and foreign keys"() {
        given: "mock database responses for table reflection"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["users"]
        
        // Mock columns query
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "users", "test_schema", "users") >> {
            args -> 
            def rowMapper = args[1] as RowMapper
            return [
                createMockColumnResult(rowMapper, "id", "integer", "integer", "NO", true),
                createMockColumnResult(rowMapper, "name", "varchar", "varchar", "YES", false),
                createMockColumnResult(rowMapper, "email", "varchar", "varchar", "NO", false)
            ]
        }
        
        // Mock foreign keys query
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "users") >> {
            args ->
            def rowMapper = args[1] as RowMapper
            return [
                createMockForeignKeyResult(rowMapper, "department_id", "departments", "id")
            ]
        }
        
        // Mock view check
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "users") >> 0

        when: "getting table schema"
        def result = schemaService.getTableSchema()

        then: "should return properly structured table info"
        result.size() == 1
        def tableInfo = result["users"]
        tableInfo.name == "users"
        tableInfo.columns.size() == 3
        tableInfo.foreignKeys.size() == 1
        !tableInfo.isView()
        
        and: "columns should be properly mapped"
        def idColumn = tableInfo.columns.find { it.name == "id" }
        idColumn.type == "integer"
        idColumn.isPrimaryKey()
        !idColumn.isNullable()
        
        def nameColumn = tableInfo.columns.find { it.name == "name" }
        nameColumn.type == "varchar"
        !nameColumn.isPrimaryKey()
        nameColumn.isNullable()
        
        and: "foreign keys should be properly mapped"
        def fk = tableInfo.foreignKeys[0]
        fk.columnName == "department_id"
        fk.referencedTable == "departments"
        fk.referencedColumn == "id"
    }

    def "should identify views correctly"() {
        given: "mock database responses for view"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["user_view"]
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "user_view", "test_schema", "user_view") >> 
            [new ColumnInfo("id", "integer", false, false)]
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "user_view") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "user_view") >> 1

        when: "getting table schema"
        def result = schemaService.getTableSchema()

        then: "should identify as view"
        def tableInfo = result["user_view"]
        tableInfo.isView()
    }

    private createMockColumnResult(RowMapper rowMapper, String columnName, String dataType, String udtName, String isNullable, boolean isPrimaryKey) {
        def mockResultSet = Mock(java.sql.ResultSet)
        mockResultSet.getString("column_name") >> columnName
        mockResultSet.getString("full_type") >> dataType  // Use full_type instead of data_type
        mockResultSet.getBoolean("is_nullable") >> (isNullable == "YES")
        mockResultSet.getBoolean("is_primary_key") >> isPrimaryKey
        
        return rowMapper.mapRow(mockResultSet, 0)
    }
    
    private createMockForeignKeyResult(RowMapper rowMapper, String columnName, String referencedTable, String referencedColumn) {
        def mockResultSet = Mock(java.sql.ResultSet)
        mockResultSet.getString("column_name") >> columnName
        mockResultSet.getString("referenced_table") >> referencedTable
        mockResultSet.getString("referenced_column") >> referencedColumn
        
        return rowMapper.mapRow(mockResultSet, 0)
    }
    
    def "should enhance PostgreSQL array types correctly"() {
        given: "mock database responses for array type"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["array_table"]
        
        // Mock columns query with array type
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "array_table", "test_schema", "array_table") >> {
            args -> 
            def rowMapper = args[1] as RowMapper
            return [
                createMockColumnResult(rowMapper, "id", "integer", "integer", "NO", true),
                createMockColumnResult(rowMapper, "tags", "varchar[]", "_varchar", "YES", false), // Array type
                createMockColumnResult(rowMapper, "scores", "integer[]", "_integer", "YES", false)
            ]
        }
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "array_table") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "array_table") >> 0

        when: "getting table schema with array types"
        def result = schemaService.getTableSchema()

        then: "should convert array types correctly"
        def tableInfo = result["array_table"]
        def tagsColumn = tableInfo.columns.find { it.name == "tags" }
        def scoresColumn = tableInfo.columns.find { it.name == "scores" }
        
        tagsColumn.type == "varchar[]"  // "_varchar" -> "varchar[]"
        scoresColumn.type == "integer[]" // "_integer" -> "integer[]"
    }
    
    def "should detect enum types correctly"() {
        given: "enum type exists"
        jdbcTemplate.queryForObject(_ as String, Boolean.class, "status_enum") >> true
        
        and: "mock database responses for enum type"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["enum_table"]
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "enum_table", "test_schema", "enum_table") >> {
            args -> 
            def rowMapper = args[1] as RowMapper
            return [
                createMockColumnResult(rowMapper, "id", "integer", "integer", "NO", true),
                createMockColumnResult(rowMapper, "status", "status_enum", "status_enum", "NO", false)
            ]
        }
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "enum_table") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "enum_table") >> 0

        when: "getting table schema with enum type"
        def result = schemaService.getTableSchema()

        then: "should identify enum type correctly"
        def tableInfo = result["enum_table"]
        def statusColumn = tableInfo.columns.find { it.name == "status" }
        statusColumn.type.startsWith("postgres_enum:status_enum")
    }
    
    def "should detect composite types correctly"() {
        given: "composite type exists"
        jdbcTemplate.queryForObject({ String sql -> sql.contains("pg_enum") }, Boolean.class, "address_type") >> false
        jdbcTemplate.queryForObject({ String sql -> sql.contains("typtype = 'c'") }, Boolean.class, "address_type") >> true
        
        and: "mock database responses for composite type"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["composite_table"]
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "composite_table", "test_schema", "composite_table") >> {
            args -> 
            def rowMapper = args[1] as RowMapper
            return [
                createMockColumnResult(rowMapper, "id", "integer", "integer", "NO", true),
                createMockColumnResult(rowMapper, "address", "address_type", "address_type", "YES", false)
            ]
        }
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "composite_table") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "composite_table") >> 0

        when: "getting table schema with composite type"
        def result = schemaService.getTableSchema()

        then: "should identify composite type correctly"
        def tableInfo = result["composite_table"]
        def addressColumn = tableInfo.columns.find { it.name == "address" }
        addressColumn.type.startsWith("postgres_composite:address_type")
    }
    
    def "should handle network types correctly"() {
        given: "mock database responses for network types"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["network_table"]
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "network_table", "test_schema", "network_table") >> {
            args -> 
            def rowMapper = args[1] as RowMapper
            return [
                createMockColumnResult(rowMapper, "id", "integer", "integer", "NO", true),
                createMockColumnResult(rowMapper, "ip_address", "inet", "inet", "YES", false),
                createMockColumnResult(rowMapper, "network_range", "cidr", "cidr", "YES", false),
                createMockColumnResult(rowMapper, "mac_address", "macaddr", "macaddr", "YES", false)
            ]
        }
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "network_table") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "network_table") >> 0

        when: "getting table schema with network types"
        def result = schemaService.getTableSchema()

        then: "should map network types correctly"
        def tableInfo = result["network_table"]
        def ipColumn = tableInfo.columns.find { it.name == "ip_address" }
        def networkColumn = tableInfo.columns.find { it.name == "network_range" }
        def macColumn = tableInfo.columns.find { it.name == "mac_address" }
        
        ipColumn.type == "inet"
        networkColumn.type == "cidr"
        macColumn.type == "macaddr"
    }
    
    def "should get enum values correctly"() {
        given: "enum values exist"
        jdbcTemplate.queryForList(_ as String, String.class, "status_type") >> 
            ["pending", "active", "inactive", "suspended"]

        when: "getting enum values"
        def result = schemaService.getEnumValues("status_type")

        then: "should return all enum values in order"
        result.size() == 4
        result == ["pending", "active", "inactive", "suspended"]
    }
    
    def "should handle enum values query failure gracefully"() {
        given: "database throws exception for enum values"
        jdbcTemplate.queryForList(_ as String, String.class, "invalid_enum") >> 
            { throw new RuntimeException("Enum not found") }

        when: "getting enum values for invalid enum"
        def result = schemaService.getEnumValues("invalid_enum")

        then: "should return empty list"
        result.isEmpty()
    }
    
    def "should get composite type definition correctly"() {
        given: "composite type definition exists"
        jdbcTemplate.query(_ as String, _ as org.springframework.jdbc.core.RowCallbackHandler, "address_type") >> {
            args ->
            def callback = args[1] as org.springframework.jdbc.core.RowCallbackHandler
            def mockResultSet1 = Mock(java.sql.ResultSet)
            mockResultSet1.getString("attname") >> "street"
            mockResultSet1.getString("typname") >> "varchar"
            callback.processRow(mockResultSet1)
            
            def mockResultSet2 = Mock(java.sql.ResultSet)
            mockResultSet2.getString("attname") >> "city"
            mockResultSet2.getString("typname") >> "varchar"
            callback.processRow(mockResultSet2)
            
            def mockResultSet3 = Mock(java.sql.ResultSet)
            mockResultSet3.getString("attname") >> "zip_code"
            mockResultSet3.getString("typname") >> "varchar"
            callback.processRow(mockResultSet3)
        }

        when: "getting composite type definition"
        def result = schemaService.getCompositeTypeDefinition("address_type")

        then: "should return field definitions in order"
        result.size() == 3
        result["street"] == "varchar"
        result["city"] == "varchar"
        result["zip_code"] == "varchar"
    }
    
    def "should handle composite type definition query failure gracefully"() {
        given: "database throws exception for composite type"
        jdbcTemplate.query(_ as String, _ as org.springframework.jdbc.core.RowCallbackHandler, "invalid_type") >> 
            { throw new RuntimeException("Type not found") }

        when: "getting definition for invalid composite type"
        def result = schemaService.getCompositeTypeDefinition("invalid_type")

        then: "should return empty map"
        result.isEmpty()
    }
    
    def "should handle table names query fallback on permission error"() {
        given: "role-based query fails but fallback succeeds"
        jdbcTemplate.queryForList({ String sql -> sql.contains("has_table_privilege") }, String.class, "test_schema") >> 
            { throw new RuntimeException("Permission check failed") }
        jdbcTemplate.queryForList({ String sql -> !sql.contains("has_table_privilege") }, String.class, "test_schema") >> 
            ["fallback_table"]

        when: "getting table names with permission error"
        def result = schemaService.reflectSchema()

        then: "should use fallback query and return tables"
        // This will trigger the private reflectSchema method through getTableSchema cache miss
        schemaService.clearCache()
        def schema = schemaService.getTableSchema()
        // The method should complete without throwing exception
        noExceptionThrown()
    }
    
    def "should handle type enhancement error gracefully"() {
        given: "type enhancement fails"
        jdbcTemplate.queryForList(_ as String, String.class, "test_schema") >> ["error_table"]
        
        // Mock enum/composite type checks to throw exceptions
        jdbcTemplate.queryForObject({ String sql -> sql.contains("pg_enum") }, Boolean.class, _) >> 
            { throw new RuntimeException("Type check failed") }
        jdbcTemplate.queryForObject({ String sql -> sql.contains("typtype = 'c'") }, Boolean.class, _) >> 
            { throw new RuntimeException("Type check failed") }
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "error_table", "test_schema", "error_table") >> {
            args -> 
            def rowMapper = args[1] as RowMapper
            return [
                createMockColumnResult(rowMapper, "id", "custom_type", "custom_type", "NO", true)
            ]
        }
        
        jdbcTemplate.query(_ as String, _ as RowMapper, "test_schema", "error_table") >> []
        jdbcTemplate.queryForObject(_ as String, Integer.class, "test_schema", "error_table") >> 0

        when: "getting table schema with type enhancement errors"
        def result = schemaService.getTableSchema()

        then: "should handle gracefully and return fallback type"
        def tableInfo = result["error_table"]
        def idColumn = tableInfo.columns.find { it.name == "id" }
        idColumn.type == "custom_type"  // Should fallback to original type
    }
}