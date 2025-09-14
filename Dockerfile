#### Stage 1: Build the application
FROM amazoncorretto:21.0.7-al2023 AS runtime

VOLUME /tmp

# Copy the JAR file from the API module target directory (assumes mvn clean install was run)
COPY modules/excalibase-rest-api/target/excalibase-rest-api-0.0.1-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-jar", "/app.jar"]