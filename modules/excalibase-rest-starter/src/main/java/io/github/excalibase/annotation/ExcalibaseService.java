package io.github.excalibase.annotation;

import org.springframework.stereotype.Service;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Custom annotation for marking Excalibase service implementations.
 *
 * <p>This annotation extends Spring's {@code @Service} annotation and provides
 * additional metadata for service identification within the Excalibase projects.
 * It allows services to be identified by a specific service name, which is used
 * for dynamic service lookup and database-specific implementation selection.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * {@code
 * @ExcalibaseService(serviceName = "postgres")
 * public class PostgresValidationService implements IValidationService {
 *     // Implementation for PostgreSQL
 * }
 * }
 * </pre>
 *
 * @see org.springframework.stereotype.Service
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Service
public @interface ExcalibaseService {
    String serviceName();
}