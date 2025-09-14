package io.github.excalibase.postgres.service

import io.github.excalibase.model.ColumnInfo
import io.github.excalibase.model.ForeignKeyInfo
import io.github.excalibase.model.SelectField
import io.github.excalibase.model.TableInfo
import io.github.excalibase.postgres.service.DatabaseSchemaService
import io.github.excalibase.postgres.service.EnhancedRelationshipService
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.util.LinkedMultiValueMap
import spock.lang.Specification

class EnhancedRelationshipServiceTest extends Specification {

    JdbcTemplate jdbcTemplate
    DatabaseSchemaService schemaService
    EnhancedRelationshipService relationshipService

    def setup() {
        jdbcTemplate = Mock(JdbcTemplate)
        schemaService = Mock(DatabaseSchemaService)
        relationshipService = new EnhancedRelationshipService(jdbcTemplate, schemaService)
    }

    def "should expand forward relationship with column selection"() {
        given: "records with foreign key references"
        def records = [
            ["id": 1, "title": "Post 1", "author_id": 101],
            ["id": 2, "title": "Post 2", "author_id": 102]
        ]
        
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [
            new SelectField("authors", [
                new SelectField("name"),
                new SelectField("email")
            ])
        ]
        
        def allTables = [
            "posts": tableInfo,
            "authors": createAuthorsTableInfo()
        ]

        when: "expanding relationships"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should query related table with selected columns"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("SELECT name, email FROM authors") &&
            sql.contains("WHERE id IN (?,?)")
        }, [101, 102].toArray()) >> [
            ["id": 101, "name": "John Doe", "email": "john@example.com"],
            ["id": 102, "name": "Jane Smith", "email": "jane@example.com"]
        ]
        
        and: "should attach related records"
        records[0]["authors"]["name"] == "John Doe"
        records[1]["authors"]["name"] == "Jane Smith"
    }

    def "should expand forward relationship with wildcard selection"() {
        given: "records and wildcard embedded field"
        def records = [
            ["id": 1, "title": "Post 1", "author_id": 101]
        ]
        
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [
            new SelectField("authors", [new SelectField("*")])
        ]
        
        def allTables = [
            "posts": tableInfo,
            "authors": createAuthorsTableInfo()
        ]

        when: "expanding with wildcard"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should query with SELECT *"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("SELECT * FROM authors")
        }, [101].toArray()) >> [
            ["id": 101, "name": "John Doe", "email": "john@example.com", "bio": "Author bio"]
        ]
        
        and: "should attach full record"
        records[0]["authors"]["name"] == "John Doe"
        records[0]["authors"]["bio"] == "Author bio"
    }

    def "should expand reverse relationship (one-to-many)"() {
        given: "author records"
        def records = [
            ["id": 101, "name": "John Doe"],
            ["id": 102, "name": "Jane Smith"]
        ]
        
        def tableInfo = createAuthorsTableInfo()
        def embeddedFields = [
            new SelectField("posts", [
                new SelectField("title"),
                new SelectField("content")
            ])
        ]
        
        def allTables = [
            "authors": tableInfo,
            "posts": createPostsTableInfo()
        ]

        when: "expanding reverse relationships"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should query posts table"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("SELECT title, content FROM posts") &&
            sql.contains("WHERE author_id IN (?,?)")
        }, [101, 102].toArray()) >> [
            ["author_id": 101, "title": "Post 1", "content": "Content 1"],
            ["author_id": 101, "title": "Post 2", "content": "Content 2"],
            ["author_id": 102, "title": "Post 3", "content": "Content 3"]
        ]
        
        and: "should group posts by author"
        records[0]["posts"].size() == 2
        records[0]["posts"][0]["title"] == "Post 1"
        records[0]["posts"][1]["title"] == "Post 2"
        records[1]["posts"].size() == 1
        records[1]["posts"][0]["title"] == "Post 3"
    }

    def "should apply embedded filters on forward relationships"() {
        given: "records with filter parameters"
        def records = [
            ["id": 1, "title": "Post 1", "author_id": 101]
        ]
        
        def tableInfo = createPostsTableInfo()
        def embeddedField = new SelectField("authors", [new SelectField("name")])
        embeddedField.addFilter("status", "eq.active")
        embeddedField.addFilter("age", "gt.25")
        
        def embeddedFields = [embeddedField]
        def allTables = ["posts": tableInfo, "authors": createAuthorsTableInfo()]

        when: "expanding with filters"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should apply filters in WHERE clause"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("SELECT name FROM authors") &&
            sql.contains("WHERE id IN (?)") &&
            sql.contains("AND status = 'active'") &&
            sql.contains("AND age > '25'")
        }, [101].toArray()) >> [
            ["id": 101, "name": "John Doe"]
        ]
    }

    def "should handle embedded filters with different operators"() {
        given: "records with various filter operators"
        def records = [["id": 1, "author_id": 101]]
        def tableInfo = createPostsTableInfo()
        
        def embeddedField = new SelectField("authors", [new SelectField("name")])
        embeddedField.addFilter("age", "gte.30")
        embeddedField.addFilter("status", "neq.inactive") 
        embeddedField.addFilter("bio", "like.writer")
        embeddedField.addFilter("score", "lt.100")
        embeddedField.addFilter("rating", "lte.5")
        
        def embeddedFields = [embeddedField]
        def allTables = ["posts": tableInfo, "authors": createAuthorsTableInfo()]

        when: "expanding with various operators"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should handle all operators correctly"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("age >= '30'") &&
            sql.contains("status != 'inactive'") &&
            sql.contains("bio LIKE '%writer%'") &&
            sql.contains("score < '100'") &&
            sql.contains("rating <= '5'")
        }, [101].toArray()) >> [["id": 101, "name": "John"]]
    }

    def "should handle empty records gracefully"() {
        given: "empty records list"
        def records = []
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [new SelectField("authors", [])]

        when: "expanding empty records"
        def result = relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should return empty list without database calls"
        0 * jdbcTemplate._
        0 * schemaService._
        result == []
    }

    def "should handle empty embedded fields gracefully"() {
        given: "records but no embedded fields"
        def records = [["id": 1, "title": "Post"]]
        def tableInfo = createPostsTableInfo()
        def embeddedFields = []

        when: "expanding with no embedded fields"
        def result = relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should return records unchanged"
        0 * jdbcTemplate._
        0 * schemaService._
        result == records
    }

    def "should handle relationship not found gracefully"() {
        given: "records with non-existent relationship"
        def records = [["id": 1, "title": "Post"]]
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [new SelectField("nonexistent", [])]
        def allTables = ["posts": tableInfo]

        when: "expanding non-existent relationship"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should handle gracefully without errors"
        1 * schemaService.getTableSchema() >> allTables
        0 * jdbcTemplate._
        noExceptionThrown()
    }

    def "should handle null foreign key values"() {
        given: "records with null foreign key"
        def records = [
            ["id": 1, "title": "Post 1", "author_id": null],
            ["id": 2, "title": "Post 2", "author_id": 101]
        ]
        
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [new SelectField("authors", [new SelectField("name")])]
        def allTables = ["posts": tableInfo, "authors": createAuthorsTableInfo()]

        when: "expanding with null foreign keys"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should only query for non-null values"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("WHERE id IN (?)")
        }, [101].toArray()) >> [["id": 101, "name": "John"]]
        
        and: "should handle null appropriately"
        records[0]["authors"] == null  // No match for null foreign key
        records[1]["authors"]["name"] == "John"
    }

    def "should handle SQL exceptions gracefully"() {
        given: "records that will cause SQL error"
        def records = [["id": 1, "author_id": 101]]
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [new SelectField("authors", [new SelectField("name")])]
        def allTables = ["posts": tableInfo, "authors": createAuthorsTableInfo()]

        when: "SQL exception occurs"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should handle exception gracefully"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList(_, _) >> { throw new RuntimeException("SQL Error") }
        noExceptionThrown()  // Should not propagate exception
    }

    def "should handle filters without operator (default to equality)"() {
        given: "embedded field with filter without operator"
        def records = [["id": 1, "author_id": 101]]
        def tableInfo = createPostsTableInfo()
        
        def embeddedField = new SelectField("authors", [new SelectField("name")])
        embeddedField.addFilter("status", "active")  // No operator, should default to equality
        
        def embeddedFields = [embeddedField]
        def allTables = ["posts": tableInfo, "authors": createAuthorsTableInfo()]

        when: "expanding with default equality filter"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should use equality operator"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("status = 'active'")
        }, [101].toArray()) >> [["id": 101, "name": "John"]]
    }

    def "should handle empty sub-fields (select all)"() {
        given: "embedded field with no sub-fields"
        def records = [["id": 1, "author_id": 101]]
        def tableInfo = createPostsTableInfo()
        def embeddedFields = [new SelectField("authors", [])]  // No sub-fields = select all
        def allTables = ["posts": tableInfo, "authors": createAuthorsTableInfo()]

        when: "expanding with empty sub-fields"
        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>())

        then: "should select all columns"
        1 * schemaService.getTableSchema() >> allTables
        1 * jdbcTemplate.queryForList({ String sql ->
            sql.contains("SELECT * FROM authors")
        }, [101].toArray()) >> [["id": 101, "name": "John", "email": "john@example.com"]]
    }

    private TableInfo createPostsTableInfo() {
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("title", "varchar", false, false),
            new ColumnInfo("content", "text", false, false),
            new ColumnInfo("author_id", "integer", false, false)
        ]
        def foreignKeys = [
            new ForeignKeyInfo("author_id", "authors", "id")
        ]
        return new TableInfo("posts", columns, foreignKeys)
    }

    private TableInfo createAuthorsTableInfo() {
        def columns = [
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("email", "varchar", false, false),
            new ColumnInfo("bio", "text", false, false)
        ]
        def foreignKeys = [
            // Reverse relationship - posts reference authors
        ]
        return new TableInfo("authors", columns, foreignKeys)
    }
}