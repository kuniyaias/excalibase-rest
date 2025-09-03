package io.github.excalibase.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Web configuration for CORS and other web-related settings
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final RestApiConfig restApiConfig;

    public WebConfig(RestApiConfig restApiConfig) {
        this.restApiConfig = restApiConfig;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        if (restApiConfig.getCors().isEnabled()) {
            registry.addMapping("/api/**")
                    .allowedOrigins(restApiConfig.getCors().getAllowedOrigins())
                    .allowedMethods(restApiConfig.getCors().getAllowedMethods())
                    .allowedHeaders(restApiConfig.getCors().getAllowedHeaders())
                    .allowCredentials(restApiConfig.getCors().isAllowCredentials())
                    .maxAge(restApiConfig.getCors().getMaxAge());
        }
    }
}