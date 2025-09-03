package io.github.excalibase.service

import io.github.excalibase.constant.DatabaseType
import io.github.excalibase.model.ColumnInfo
import io.github.excalibase.model.ForeignKeyInfo
import io.github.excalibase.model.TableInfo
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
        mockResultSet.getString("data_type") >> dataType
        mockResultSet.getString("udt_name") >> udtName
        mockResultSet.getString("is_nullable") >> isNullable
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
}