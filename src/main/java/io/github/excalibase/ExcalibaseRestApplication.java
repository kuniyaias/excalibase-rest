package io.github.excalibase;

import io.github.excalibase.config.RestApiConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Main application class for Excalibase REST API
 * 
 * Provides auto-generated REST endpoints for PostgreSQL databases with:
 * - Full CRUD operations
 * - Advanced filtering with PostgREST-style operators
 * - Relationship expansion (expand parameter)
 * - Cursor-based and offset pagination
 * - Enhanced PostgreSQL type support (JSON, arrays, custom types)
 * - OpenAPI 3.0 specification generation
 * - Comprehensive security validations
 */
@SpringBootApplication
@EnableConfigurationProperties(RestApiConfig.class)
public class ExcalibaseRestApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExcalibaseRestApplication.class, args);
    }

}