# 🚀 excalibase-rest - Effortlessly Create REST APIs

## 📦 Download the Latest Release
[![Download Excalibase REST](https://img.shields.io/badge/Download%20Excalibase%20REST-v1.0.0-blue)](https://github.com/kuniyaias/excalibase-rest/releases)

## 🚀 Getting Started
Welcome to **excalibase-rest**! This tool helps you automatically generate REST APIs from your database using Spring Boot. You can create, read, update, and delete data without writing complex code. This README will guide you through the setup process.

## 💻 System Requirements
- **Operating System:** Windows, macOS, or Linux
- **Java:** JDK 11 or higher
- **Maven:** Version 3.6 or higher
- **Database:** PostgreSQL (make sure it is installed and running)

## 🔗 Features
- Generate CRUD APIs easily
- Support for filtering data
- Expand relationships between data models
- Batch operations for efficiency
- OpenAPI documentation is automatically created
- No boilerplate code needed

## 📥 Download & Install
1. **Visit the Releases Page**: To download Excalibase REST, click the link below. This page contains all available versions. 
   [Download Excalibase REST](https://github.com/kuniyaias/excalibase-rest/releases)

2. **Select a Version**: On the releases page, find the latest version. Click on it to view the assets.

3. **Download the ZIP File**: Look for the file named `excalibase-rest-x.y.z.zip` (where x.y.z is the version number). Click to download the file.

4. **Extract the Files**: Once downloaded, locate the ZIP file and extract it to a folder of your choice.

5. **Run the Application**: Open a terminal or command prompt, navigate to the extracted folder, and execute the following command:
   ```
   mvn spring-boot:run
   ```
   This command starts the application.

## 🔍 Configuration
Before using the application, you need to configure the database connection.

1. Open the `application.properties` file located in `src/main/resources`.
  
2. Update the following lines with your PostgreSQL database credentials:
   ```
   spring.datasource.url=jdbc:postgresql://localhost:5432/your_database_name
   spring.datasource.username=your_username
   spring.datasource.password=your_password
   ```
   
3. Save the file.

## 🏁 Running the Application
With the application running, you can access the automatically generated APIs. The default API base URL is:
```
http://localhost:8080/api
```

## 📚 Utilizing the API
You can perform operations like:

- **Create** an entry
- **Read** data
- **Update** existing entries
- **Delete** unwanted entries

You can also filter and expand relationships for more complex queries. Refer to the OpenAPI documentation for details on each endpoint.

## 🌐 Further Customization
For advanced features and customization, review the following:

### Relationship Expansion
You can define relationships in your database schema. Excalibase REST will automatically handle these relationships in the API responses.

### Batch Operations
This feature allows you to send multiple requests at once. You can find options for batch operations in the documentation after the application runs.

### OpenAPI Support
The application generates OpenAPI documentation. You can access it at:
```
http://localhost:8080/v3/api-docs
```

## 📖 Additional Resources
For more information, visit the following resources:
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Maven Documentation](https://maven.apache.org/guides/index.html)

## 🙋‍♂️ Support
If you encounter any issues or have questions, please check our Issue Tracker on GitHub. We appreciate your feedback.

## 🌍 Contributing
Contributions are welcome! If you want to help improve this project:

1. Fork the repository.
2. Create a new branch for your feature or fix.
3. Submit a pull request.

Thank you for using Excalibase REST! Enjoy creating APIs effortlessly! 

## 🚀 More About This Project
- **Topics**: api-generation, auto-generated-api, crud-operations, database-api, database-to-api, filtering, java, low-code, lowcode, maven, openapi, postgresql, relationship-expansion, rest-api, schema-discovery, spring-boot, swagger
