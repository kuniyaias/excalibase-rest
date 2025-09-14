package io.github.excalibase.controller

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.servlet.MockMvc
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import static org.hamcrest.Matchers.*

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class CompositeKeyIntegrationTest extends Specification {

    @Shared
    PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:15-alpine")
            .withDatabaseName("composite_testdb")
            .withUsername("composite_user")
            .withPassword("composite_pass")

    @Autowired
    MockMvc mockMvc

    @Autowired
    JdbcTemplate jdbcTemplate

    def setupSpec() {
        postgres.start()
        
        // Set unique system properties for this test
        System.setProperty("spring.datasource.url", postgres.getJdbcUrl())
        System.setProperty("spring.datasource.username", postgres.getUsername())
        System.setProperty("spring.datasource.password", postgres.getPassword())
        System.setProperty("spring.datasource.driver-class-name", "org.postgresql.Driver")
    }

    def setup() {
        setupTestData()
    }

    def cleanup() {
        cleanupTestData()
    }

    def cleanupSpec() {
        postgres.stop()
        
        // Clear system properties to prevent interference with other tests
        System.clearProperty("spring.datasource.url")
        System.clearProperty("spring.datasource.username")
        System.clearProperty("spring.datasource.password")
        System.clearProperty("spring.datasource.driver-class-name")
    }

    private void setupTestData() {
        // Create order_items table with composite primary key
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                unit_price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (order_id, product_id)
            )
        """)

        // Insert test data
        jdbcTemplate.update(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)", 
            1, 2, 5, 99.99
        )
        jdbcTemplate.update(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)", 
            1, 3, 2, 49.99
        )
        jdbcTemplate.update(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)", 
            2, 2, 1, 99.99
        )
    }

    private void cleanupTestData() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS order_items CASCADE")
    }

    def "should get record by composite key"() {
        when: "requesting record by composite key"
        def result = mockMvc.perform(get("/api/v1/order_items/1,2"))

        then: "should return the record"
        result.andExpect(status().isOk())
              .andExpect(content().contentType(MediaType.APPLICATION_JSON))
              .andExpect(jsonPath('$.order_id').value(1))
              .andExpect(jsonPath('$.product_id').value(2))
              .andExpect(jsonPath('$.quantity').value(5))
              .andExpect(jsonPath('$.unit_price').value(99.99))
    }

    def "should update record by composite key"() {
        when: "updating record by composite key"
        def result = mockMvc.perform(patch("/api/v1/order_items/1,2")
                .contentType(MediaType.APPLICATION_JSON)
                .content('{"quantity": 10}'))

        then: "should return updated record"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.order_id').value(1))
              .andExpect(jsonPath('$.product_id').value(2))
              .andExpect(jsonPath('$.quantity').value(10))
              .andExpect(jsonPath('$.unit_price').value(99.99))
    }

    def "should delete record by composite key"() {
        when: "deleting record by composite key"
        def result = mockMvc.perform(delete("/api/v1/order_items/1,3"))

        then: "should return no content"
        result.andExpect(status().isNoContent())

        and: "record should be deleted"
        def count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM order_items WHERE order_id = ? AND product_id = ?",
            Integer.class, 1, 3
        )
        count == 0
    }

    // Note: Composite key validation tests are covered in unit tests
    // Integration tests focus on end-to-end functionality with real database

    def "should return 404 for non-existent composite key"() {
        when: "requesting non-existent composite key"
        def result = mockMvc.perform(get("/api/v1/order_items/999,888"))

        then: "should return 404"
        result.andExpect(status().isNotFound())
              .andExpect(jsonPath('$.error').value("Record not found"))
    }

    def "should handle composite key in queries"() {
        when: "getting all order_items"
        def result = mockMvc.perform(get("/api/v1/order_items"))

        then: "should return all records"
        result.andExpect(status().isOk())
              .andExpect(jsonPath('$.data').isArray())
              .andExpect(jsonPath('$.data').value(hasSize(3)))
              .andExpect(jsonPath('$.pagination.total').value(3))
    }

    def "should create record with composite key data"() {
        when: "creating new order item"
        def result = mockMvc.perform(post("/api/v1/order_items")
                .contentType(MediaType.APPLICATION_JSON)
                .content('{"order_id": 3, "product_id": 4, "quantity": 7, "unit_price": 199.99}'))

        then: "should return created record"
        result.andExpect(status().isCreated())
              .andExpect(jsonPath('$.order_id').value(3))
              .andExpect(jsonPath('$.product_id').value(4))
              .andExpect(jsonPath('$.quantity').value(7))
              .andExpect(jsonPath('$.unit_price').value(199.99))
    }
}
