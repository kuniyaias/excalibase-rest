# Excalibase REST API

A standalone REST API for PostgreSQL databases that automatically generates endpoints from your database schema with advanced filtering, relationships, and comprehensive PostgreSQL type support.

## 🚀 Features

- **Auto-Generated Endpoints**: Automatically creates REST endpoints for all tables in your PostgreSQL database
- **Advanced Filtering**: PostgREST-style filtering with 15+ operators (`eq`, `gt`, `like`, `in`, `haskey`, `arraycontains`, etc.)
- **Relationship Expansion**: Traverse foreign key relationships with the `expand` parameter
- **Dual Pagination**: Both offset-based and cursor-based (GraphQL connections style) pagination
- **Enhanced PostgreSQL Types**: Full support for JSON, arrays, network types, custom enums, and composite types
- **Bulk Operations**: Create, update, and delete multiple records in a single request
- **OpenAPI 3.0**: Auto-generated OpenAPI specification with interactive documentation
- **Security**: Built-in SQL injection protection and input validation

## 📋 Quick Start

### Prerequisites

- Java 21+
- PostgreSQL 12+
- Maven 3.8+

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/excalibase/excalibase-rest.git
   cd excalibase-rest
   ```

2. **Configure your database connection:**
   ```yaml
   # src/main/resources/application.yaml
   spring:
     datasource:
       url: jdbc:postgresql://localhost:5432/your_database
       username: your_username
       password: your_password
   
   app:
     allowed-schema: your_schema  # default: public
   ```

3. **Build and run:**
   ```bash
   mvn clean install
   mvn spring-boot:run
   ```

4. **Access the API:**
   - API Base URL: `http://localhost:8080/api/v1`
   - OpenAPI Spec: `http://localhost:8080/api/v1/openapi.json`
   - Documentation: `http://localhost:8080/api/v1/docs`

## 📚 API Usage

### Basic CRUD Operations

```bash
# Get all customers
GET /api/v1/customer

# Get customer by ID
GET /api/v1/customer/123

# Create a new customer
POST /api/v1/customer
{
  "name": "John Doe",
  "email": "john@example.com"
}

# Update customer
PUT /api/v1/customer/123
{
  "name": "John Smith",
  "email": "johnsmith@example.com"
}

# Partial update
PATCH /api/v1/customer/123
{
  "email": "newemail@example.com"
}

# Delete customer
DELETE /api/v1/customer/123
```

### Advanced Filtering

Excalibase REST uses PostgREST-style filtering operators:

```bash
# Basic operators
GET /api/v1/orders?total=gte.100           # total >= 100
GET /api/v1/users?age=gt.18                # age > 18
GET /api/v1/products?name=like.phone       # name LIKE '%phone%'
GET /api/v1/items?status=in.(pending,shipped)  # status IN ('pending', 'shipped')

# Enhanced PostgreSQL operations
GET /api/v1/users?profile=haskey.admin     # JSON: profile ? 'admin'
GET /api/v1/posts?tags=arraycontains.tech  # Array: tags @> ARRAY['tech']
GET /api/v1/logs?data=jsoncontains.{"level":"error"}  # JSONB contains

# OR conditions
GET /api/v1/users?or=(age.gte.65,vip.is.true)  # age >= 65 OR vip = true

# String operations
GET /api/v1/products?name=startswith.iPhone  # name LIKE 'iPhone%'
GET /api/v1/emails?address=endswith.gmail    # address LIKE '%gmail'
```

### Relationship Expansion

Expand related data using foreign key relationships:

```bash
# Expand customer information in orders
GET /api/v1/orders?expand=customer

# Expand multiple relationships
GET /api/v1/orders?expand=customer,items

# Parameterized expansion
GET /api/v1/customers?expand=orders(limit:5,select:total,status)

# Response includes related data
{
  "data": [{
    "order_id": 1,
    "customer_id": 123,
    "total": 99.99,
    "customer": {
      "customer_id": 123,
      "name": "John Doe",
      "email": "john@example.com"
    }
  }]
}
```

### Pagination

#### Offset-based Pagination
```bash
GET /api/v1/products?offset=20&limit=10
```

#### Cursor-based Pagination (GraphQL Connections)
```bash
# Forward pagination
GET /api/v1/products?first=10&after=cursor123

# Backward pagination  
GET /api/v1/products?last=10&before=cursor456

# Response format
{
  "edges": [
    {
      "node": { "id": 1, "name": "Product" },
      "cursor": "eyJpZCI6MX0="
    }
  ],
  "pageInfo": {
    "hasNextPage": true,
    "hasPreviousPage": false,
    "startCursor": "eyJpZCI6MX0=",
    "endCursor": "eyJpZCI6MTB9"
  },
  "totalCount": 150
}
```

### Bulk Operations

```bash
# Bulk create
POST /api/v1/customers
[
  {"name": "John", "email": "john@example.com"},
  {"name": "Jane", "email": "jane@example.com"}
]

# Response
{
  "data": [...],
  "count": 2
}
```

### Field Selection

```bash
# Select specific columns
GET /api/v1/customers?select=name,email

# Combined with relationships
GET /api/v1/orders?select=total,status&expand=customer(select:name,email)
```

### Sorting

```bash
# Simple sorting
GET /api/v1/products?orderBy=name&orderDirection=desc

# PostgREST-style sorting with multiple columns
GET /api/v1/orders?order=created_at.desc,total.asc
```

## 🗄️ Enhanced PostgreSQL Type Support

Excalibase REST provides comprehensive support for PostgreSQL's advanced data types:

### JSON/JSONB Operations
```bash
# Check if JSON has key
GET /api/v1/users?profile=haskey.preferences

# Check for multiple keys
GET /api/v1/documents?metadata=haskeys.["author","title"]

# JSON contains query
GET /api/v1/configs?settings=jsoncontains.{"theme":"dark"}
```

### Array Operations
```bash
# Array contains element
GET /api/v1/posts?tags=arraycontains.technology

# Array has any of these elements
GET /api/v1/users?skills=arrayhasany.["python","java"]

# Array contains all elements
GET /api/v1/products?features=arrayhasall.["waterproof","wireless"]

# Filter by array length
GET /api/v1/lists?items=arraylength.5
```

### Network Types
```bash
# Basic string operations work with INET, CIDR, MACADDR
GET /api/v1/servers?ip_address=like.192.168%
```

### Custom Types
```bash
# Enum values work as strings
GET /api/v1/orders?status=eq.pending

# Composite types work as JSON objects
POST /api/v1/addresses
{
  "location": {
    "street": "123 Main St",
    "city": "Anytown",
    "state": "CA"
  }
}
```

## 🔧 Configuration

### Application Configuration

```yaml
# application.yaml
app:
  # Database configuration
  allowed-schema: public
  database-type: postgres
  
  # Pagination limits
  max-page-size: 1000
  default-page-size: 100
  
  # Cache settings
  schema-cache-ttl-seconds: 300
  
  # CORS configuration
  cors:
    enabled: true
    allowed-origins: ["*"]
    allowed-methods: ["GET", "POST", "PUT", "PATCH", "DELETE"]
  
  # Security settings  
  security:
    enable-sql-injection-protection: true
    enable-table-name-validation: true
    max-request-body-size: 1048576
```

### Environment Variables

```bash
export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/mydb
export SPRING_DATASOURCE_USERNAME=myuser
export SPRING_DATASOURCE_PASSWORD=mypass
export APP_ALLOWED_SCHEMA=myschema
```

## 📖 API Documentation

### OpenAPI Specification

- **JSON**: `GET /api/v1/openapi.json`
- **YAML**: `GET /api/v1/openapi.yaml`
- **Documentation Info**: `GET /api/v1/docs`

### Schema Introspection

```bash
# List all tables
GET /api/v1

# Get table schema
GET /api/v1/customers/schema
```

### Interactive Documentation

Import the OpenAPI specification into:
- **Swagger UI**: Use the `/openapi.json` URL
- **Postman**: Import as OpenAPI 3.0 collection
- **Insomnia**: Import as OpenAPI specification

## 🛡️ Security Features

- **SQL Injection Protection**: Parameterized queries and input validation
- **Table Name Validation**: Prevents access to unauthorized tables
- **Column Name Validation**: Validates column names against schema
- **Request Size Limits**: Configurable maximum request body size
- **CORS Support**: Configurable cross-origin resource sharing

## 🏗️ Development

### Building

```bash
# Clean build
mvn clean install

# Skip tests
mvn clean install -DskipTests

# Run tests only
mvn test
```

### Running Tests

```bash
# All tests
mvn test

# Integration tests with Testcontainers
mvn test -Dtest="*IntegrationTest"
```

### Code Quality

```bash
# Generate test coverage report
mvn clean test jacoco:report

# Generate Javadoc
mvn javadoc:javadoc
```

## 🔍 Monitoring

The application includes Spring Boot Actuator endpoints:

```bash
# Health check
GET /actuator/health

# Application info
GET /actuator/info

# Metrics
GET /actuator/metrics
```

## 🚀 Performance

- **Connection Pooling**: HikariCP with optimized settings
- **Schema Caching**: Configurable TTL cache for database metadata
- **Efficient Queries**: N+1 query prevention in relationship expansion
- **Pagination**: Built-in limits to prevent large result sets

## 📝 Examples

### E-commerce API

```bash
# Get products with inventory and category info
GET /api/v1/products?expand=category,inventory&status=eq.active

# Get customer orders with items
GET /api/v1/customers/123?expand=orders(expand:items,limit:10)

# Search products by name and filter by price range
GET /api/v1/products?name=like.laptop&price=gte.500&price=lte.2000

# Get recent orders with customer details
GET /api/v1/orders?created_at=gte.2024-01-01&expand=customer&order=created_at.desc
```

### Analytics Queries

```bash
# Users with specific preferences
GET /api/v1/users?preferences=haskey.notifications

# Posts with multiple tags
GET /api/v1/posts?tags=arrayhasall.["tech","news"]

# Configuration with specific settings
GET /api/v1/configs?settings=jsoncontains.{"enabled":true}
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Inspired by [PostgREST](https://postgrest.org/) for API design patterns
- Built with Spring Boot and Java 21
- PostgreSQL for advanced database features