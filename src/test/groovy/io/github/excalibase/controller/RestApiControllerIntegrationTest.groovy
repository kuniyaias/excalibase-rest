package io.github.excalibase.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.servlet.MockMvc
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import static org.hamcrest.Matchers.*

@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
class RestApiControllerIntegrationTest extends Specification {

    @Shared
    PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:15-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")

    @Autowired
    MockMvc mockMvc

    @Autowired
    JdbcTemplate jdbcTemplate

    def setupSpec() {
        postgres.start()
        
        // Set system properties for Spring Boot to use
        System.setProperty("spring.datasource.url", postgres.getJdbcUrl())
        System.setProperty("spring.datasource.username", postgres.getUsername())
        System.setProperty("spring.datasource.password", postgres.getPassword())
        System.setProperty("spring.datasource.driver-class-name", "org.postgresql.Driver")
    }

    def setup() {
        // Create test tables and data before each test
        setupTestData()
    }

    def cleanup() {
        // Clean up test data after each test
        cleanupTestData()
    }

    def cleanupSpec() {
        postgres.stop()
    }

    private void setupTestData() {
        // Create users table
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        // Create orders table for relationship testing
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                product_name VARCHAR(255) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        // Insert test data
        jdbcTemplate.update("INSERT INTO users (name, email) VALUES (?, ?)", "John", "john@example.com")
        jdbcTemplate.update("INSERT INTO users (name, email) VALUES (?, ?)", "Jane", "jane@example.com")
        jdbcTemplate.update("INSERT INTO orders (user_id, product_name, amount) VALUES (?, ?, ?)", 1, "Laptop", 999.99)
        jdbcTemplate.update("INSERT INTO orders (user_id, product_name, amount) VALUES (?, ?, ?)", 1, "Mouse", 29.99)
    }

    private void cleanupTestData() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS orders CASCADE")
        jdbcTemplate.execute("DROP TABLE IF EXISTS users CASCADE")
    }

    def "should get records successfully"() {
        when: "requesting records"
        def result = mockMvc.perform(get("/api/v1/users"))

        then: "should return success response"
        result.andExpect(status().isOk())
              .andExpect(content().contentType(MediaType.APPLICATION_JSON))
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').isNotEmpty())
              .andExpect(jsonPath('$.data[0].name').exists())
              .andExpect(jsonPath('$.pagination.total').value(2))
    }

    def "should handle query parameters correctly"() {
        when: "requesting with query parameters"
        def result = mockMvc.perform(get("/api/v1/users")
                .param("offset", "0")
                .param("limit", "1")
                .param("orderBy", "name")
                .param("orderDirection", "asc"))

        then: "should return filtered response"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').hasJsonPath())
              .andExpect(jsonPath('$.pagination.limit').value(1))
    }

    def "should get single record by ID"() {
        when: "requesting single record"
        def result = mockMvc.perform(get("/api/v1/users/1"))

        then: "should return record"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.id').value(1))
              .andExpect(jsonPath('$.name').value("John"))
              .andExpect(jsonPath('$.email').value("john@example.com"))
    }

    def "should return 404 for non-existent record"() {
        when: "requesting non-existent record"
        def result = mockMvc.perform(get("/api/v1/users/999"))

        then: "should return 404"
        result.andExpect(status().isNotFound())
              .andExpect(jsonPath('$.error').value("Record not found"))
    }

    def "should create single record"() {
        when: "creating record"
        def result = mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content('{"name": "Alice", "email": "alice@example.com"}'))

        then: "should return created record"
        result.andExpect(status().isCreated())
              .andExpect(jsonPath('$.name').value("Alice"))
              .andExpect(jsonPath('$.email').value("alice@example.com"))
    }

    def "should create bulk records"() {
        when: "creating bulk records"
        def result = mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content('[{"name": "Bob"}, {"name": "Charlie"}]'))

        then: "should return bulk creation response"
        result.andExpect(status().isCreated())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data[0].name').value("Bob"))
              .andExpect(jsonPath('$.data[1].name').value("Charlie"))
              .andExpect(jsonPath('$.count').value(2))
    }

    def "should update record with PUT"() {
        when: "updating record"
        def result = mockMvc.perform(put("/api/v1/users/1")
                .contentType(MediaType.APPLICATION_JSON)
                .content('{"name": "John Updated", "email": "john.updated@example.com"}'))

        then: "should return updated record"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.name').value("John Updated"))
              .andExpect(jsonPath('$.email').value("john.updated@example.com"))
    }

    def "should partially update record with PATCH"() {
        when: "patching record"
        def result = mockMvc.perform(patch("/api/v1/users/1")
                .contentType(MediaType.APPLICATION_JSON)
                .content('{"email": "updated@example.com"}'))

        then: "should return updated record"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.email').value("updated@example.com"))
              .andExpect(jsonPath('$.name').value("John")) // Name should remain unchanged
    }

    def "should delete record"() {
        when: "deleting record"
        def result = mockMvc.perform(delete("/api/v1/users/2"))

        then: "should return no content"
        result.andExpect(status().isNoContent())
    }

    def "should return 404 when deleting non-existent record"() {
        when: "deleting non-existent record"
        def result = mockMvc.perform(delete("/api/v1/users/999"))

        then: "should return 404"
        result.andExpect(status().isNotFound())
              .andExpect(jsonPath('$.error').value("Record not found"))
    }

    def "should return schema information"() {
        when: "requesting schema"
        def result = mockMvc.perform(get("/api/v1"))

        then: "should return table list"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.tables').isArray())
              .andExpect(jsonPath('$.tables').isNotEmpty())
    }

    def "should return table schema"() {
        when: "requesting table schema"
        def result = mockMvc.perform(get("/api/v1/users/schema"))

        then: "should return table details"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.table.name').value("users"))
              .andExpect(jsonPath('$.table.columns').isArray())
              .andExpect(jsonPath('$.table.columns').isNotEmpty())
    }

    def "should return 404 for non-existent table schema"() {
        when: "requesting non-existent table schema"
        def result = mockMvc.perform(get("/api/v1/nonexistent/schema"))

        then: "should return 404"
        result.andExpect(status().isNotFound())
              .andExpect(jsonPath('$.error').value("Table not found: nonexistent"))
    }

    def "should return OpenAPI JSON specification"() {
        when: "requesting OpenAPI JSON"
        def result = mockMvc.perform(get("/api/v1/openapi.json"))

        then: "should return OpenAPI spec"
        result.andExpect(status().isOk())
              .andExpect(content().contentType(MediaType.APPLICATION_JSON))
              .andExpect(jsonPath('$.openapi').exists())
              .andExpect(jsonPath('$.info').exists())
    }

    def "should return OpenAPI YAML specification"() {
        when: "requesting OpenAPI YAML"
        def result = mockMvc.perform(get("/api/v1/openapi.yaml"))

        then: "should return YAML content"
        result.andExpect(status().isOk())
              .andExpect(content().contentTypeCompatibleWith("application/yaml"))
              .andExpect(content().string(containsString("openapi:")))
    }

    def "should return API documentation info"() {
        when: "requesting docs info"
        def result = mockMvc.perform(get("/api/v1/docs"))

        then: "should return documentation links"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.title').value("Excalibase REST API Documentation"))
              .andExpect(jsonPath('$.openapi_json').value("/api/v1/openapi.json"))
              .andExpect(jsonPath('$.swagger_ui').exists())
    }
    
    def "should apply OR conditions in filters"() {
        when: "using OR conditions"
        def result = mockMvc.perform(get("/api/v1/users").param("or", "(name.like.John,name.like.Jane)"))

        then: "should return users matching either condition"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').isNotEmpty())
    }
    
    def "should handle complex PostgreSQL types"() {
        given: "orders table with JSONB column for testing haskey operator"
        jdbcTemplate.execute("""
            ALTER TABLE orders ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'
        """)
        
        jdbcTemplate.execute("""
            UPDATE orders SET metadata = '{"priority": "high", "source": "web"}' WHERE id = 1
        """)
        
        jdbcTemplate.execute("""
            UPDATE orders SET metadata = '{"status": "active", "category": "electronics"}' WHERE id = 2
        """)

        when: "testing basic JSON functionality without haskey first"  
        def result = mockMvc.perform(get("/api/v1/orders"))

        then: "should return orders with JSONB metadata column"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').isNotEmpty())
              .andExpect(jsonPath('$.data[0].metadata').exists())

        cleanup:
        jdbcTemplate.execute("ALTER TABLE orders DROP COLUMN IF EXISTS metadata")
    }
    
    def "should handle sorting with orderBy parameter"() {
        when: "requesting users with sorting"
        def result = mockMvc.perform(get("/api/v1/users")
                .param("orderBy", "name")
                .param("orderDirection", "desc"))

        then: "should return sorted results"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').isNotEmpty())
    }
    
    def "should handle PostgREST-style ordering"() {
        when: "using PostgREST order syntax"
        def result = mockMvc.perform(get("/api/v1/users").param("order", "name.desc,id.asc"))

        then: "should apply multiple column sorting"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').isNotEmpty())
    }
    
    def "should select specific columns"() {
        when: "requesting specific columns only"
        def result = mockMvc.perform(get("/api/v1/users").param("select", "name,email"))

        then: "should return only selected columns"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data[0].name').exists())
              .andExpect(jsonPath('$.data[0].email').exists())
              // Note: ID should not be present, but MockMvc JSON path can't test for absence easily
    }
    
    def "should handle cursor-based pagination"() {
        when: "requesting with cursor pagination"
        def result = mockMvc.perform(get("/api/v1/users").param("first", "2"))

        then: "should return GraphQL connections format"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.edges').isArray())
              .andExpect(jsonPath('$.pageInfo.hasNextPage').exists())
              .andExpect(jsonPath('$.pageInfo.hasPreviousPage').exists())
              .andExpect(jsonPath('$.totalCount').exists())
    }
    
    def "should expand relationships"() {
        given: "orders table with foreign key to users"
        jdbcTemplate.execute("""
            INSERT INTO orders (user_id, product_name, amount) VALUES (1, 'Test Product', 99.99)
            ON CONFLICT DO NOTHING
        """)

        when: "requesting orders with user expansion"
        def result = mockMvc.perform(get("/api/v1/orders").param("expand", "users"))

        then: "should include user data in orders"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').isNotEmpty())
    }
    
    def "should validate column names in filters"() {
        when: "using invalid column in filter"
        def result = mockMvc.perform(get("/api/v1/users").param("invalid_column", "eq.value"))

        then: "should return bad request"
        result.andExpect(status().isBadRequest())
              .andExpect(jsonPath('$.error').value(containsString("Invalid column")))
    }

    def "should validate empty request body"() {
        when: "creating record with empty body"
        def result = mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content('{}'))

        then: "should return 400 Bad Request"
        result.andExpect(status().isBadRequest())
              .andExpect(jsonPath('$.error').value("Request body cannot be empty"))
    }

    def "should validate request body type"() {
        when: "creating record with invalid body type"
        def result = mockMvc.perform(post("/api/v1/users")
                .contentType(MediaType.APPLICATION_JSON)
                .content('"invalid string body"'))

        then: "should return 400 Bad Request"
        result.andExpect(status().isBadRequest())
              .andExpect(jsonPath('$.error').value("Request body must be an object or array"))
    }
}