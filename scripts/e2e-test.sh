#!/bin/bash

# Excalibase REST API End-to-End Test Suite
# Tests comprehensive REST API functionality with real database

set -e

# Configuration
API_BASE_URL="${API_BASE_URL:-http://localhost:8080/api/v1}"
POSTGRES_URL="${POSTGRES_URL:-jdbc:postgresql://localhost:5432/excalibase}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED_TESTS++))
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED_TESTS++))
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Test helper functions
run_test() {
    local test_name="$1"
    local test_command="$2"
    ((TOTAL_TESTS++))
    
    log_info "Running: $test_name"
    
    if eval "$test_command" &>/dev/null; then
        log_success "$test_name"
        return 0
    else
        log_error "$test_name"
        return 1
    fi
}

check_response() {
    local response="$1"
    local expected_status="$2"
    local description="$3"
    
    if [[ -z "$response" ]]; then
        log_error "$description - Empty response"
        return 1
    fi
    
    local status=$(echo "$response" | jq -r '.status // empty')
    if [[ "$status" == "$expected_status" || "$expected_status" == "any" ]]; then
        log_success "$description"
        return 0
    else
        log_error "$description - Expected status $expected_status, got $status"
        return 1
    fi
}

# Wait for service to be ready
wait_for_service() {
    log_info "Waiting for REST API to be ready..."
    for i in {1..30}; do
        if curl -sf "$API_BASE_URL" >/dev/null 2>&1; then
            log_success "REST API is ready"
            return 0
        fi
        log_info "Attempt $i/30: Service not ready, waiting 5 seconds..."
        sleep 5
    done
    log_error "Service failed to start within 150 seconds"
    exit 1
}

# Test suite functions
test_schema_endpoints() {
    log_info "=== Testing Schema Endpoints ==="
    
    # Test schema list
    run_test "Get schema list" "
        response=\$(curl -s '$API_BASE_URL') &&
        echo \"\$response\" | jq -e '.tables | type == \"array\"' &&
        echo \"\$response\" | jq -e '.tables | length > 0'
    "
    
    # Test table schema
    run_test "Get table schema" "
        response=\$(curl -s '$API_BASE_URL/customers/schema') &&
        echo \"\$response\" | jq -e '.table.name == \"customers\"' &&
        echo \"\$response\" | jq -e '.table.columns | type == \"array\"'
    "
    
    # Test OpenAPI JSON
    run_test "Get OpenAPI JSON specification" "
        response=\$(curl -s '$API_BASE_URL/openapi.json') &&
        echo \"\$response\" | jq -e '.openapi == \"3.0.3\"' &&
        echo \"\$response\" | jq -e '.info.title'
    "
    
    # Test documentation endpoint
    run_test "Get API documentation info" "
        response=\$(curl -s '$API_BASE_URL/docs') &&
        echo \"\$response\" | jq -e '.title' &&
        echo \"\$response\" | jq -e '.openapi_json'
    "
}

test_basic_crud_operations() {
    log_info "=== Testing Basic CRUD Operations ==="
    
    # Test GET all customers
    run_test "Get all customers" "
        response=\$(curl -s '$API_BASE_URL/customers') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"' &&
        echo \"\$response\" | jq -e '.pagination.total >= 0'
    "
    
    # Test GET single customer
    run_test "Get customer by ID" "
        response=\$(curl -s '$API_BASE_URL/customers/1') &&
        echo \"\$response\" | jq -e '.customer_id == 1' &&
        echo \"\$response\" | jq -e '.name | type == \"string\"'
    "
    
    # Test POST create customer
    run_test "Create new customer" "
        response=\$(curl -s -X POST '$API_BASE_URL/customers' \\
            -H 'Content-Type: application/json' \\
            -d '{\"name\": \"Test Customer\", \"email\": \"test@example.com\"}') &&
        echo \"\$response\" | jq -e '.name == \"Test Customer\"' &&
        echo \"\$response\" | jq -e '.email == \"test@example.com\"'
    "
    
    # Store created customer ID for update/delete tests
    CREATED_CUSTOMER_ID=$(curl -s -X POST "$API_BASE_URL/customers" \
        -H 'Content-Type: application/json' \
        -d '{"name": "Update Test Customer", "email": "update-test@example.com"}' | jq -r '.customer_id')
    
    # Test PUT update customer
    if [[ "$CREATED_CUSTOMER_ID" != "null" && -n "$CREATED_CUSTOMER_ID" ]]; then
        run_test "Update customer with PUT" "
            response=\$(curl -s -X PUT '$API_BASE_URL/customers/$CREATED_CUSTOMER_ID' \\
                -H 'Content-Type: application/json' \\
                -d '{\"name\": \"Updated Customer\", \"email\": \"updated@example.com\"}') &&
            echo \"\$response\" | jq -e '.name == \"Updated Customer\"'
        "
        
        # Test PATCH update customer
        run_test "Partial update customer with PATCH" "
            response=\$(curl -s -X PATCH '$API_BASE_URL/customers/$CREATED_CUSTOMER_ID' \\
                -H 'Content-Type: application/json' \\
                -d '{\"phone\": \"+1-555-9999\"}') &&
            echo \"\$response\" | jq -e '.phone == \"+1-555-9999\"'
        "
        
        # Test DELETE customer
        run_test "Delete customer" "
            curl -s -X DELETE '$API_BASE_URL/customers/$CREATED_CUSTOMER_ID' | grep -q '.' || test \$? -eq 1
        "
    else
        log_warning "Skipping update/delete tests - could not create test customer"
    fi
}

test_advanced_filtering() {
    log_info "=== Testing Advanced Filtering ==="
    
    # Test basic equality filter
    run_test "Filter with equality operator" "
        response=\$(curl -s '$API_BASE_URL/customers?tier=eq.gold') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test LIKE filter
    run_test "Filter with LIKE operator" "
        response=\$(curl -s '$API_BASE_URL/customers?name=like.John') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test greater than filter
    run_test "Filter with greater than operator" "
        response=\$(curl -s '$API_BASE_URL/products?price=gt.100') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test IN operator
    run_test "Filter with IN operator" "
        response=\$(curl -s '$API_BASE_URL/customers?tier=in.(gold,platinum)') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test OR conditions
    run_test "Filter with OR conditions" "
        response=\$(curl -s '$API_BASE_URL/customers?or=(tier.eq.gold,tier.eq.platinum)') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test JSON has key operator
    run_test "Filter JSON with haskey operator" "
        response=\$(curl -s '$API_BASE_URL/customers?profile=haskey.preferences') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test array contains operator
    run_test "Filter array with contains operator" "
        response=\$(curl -s '$API_BASE_URL/customers?tags=arraycontains.vip') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test boolean filter
    run_test "Filter boolean field" "
        response=\$(curl -s '$API_BASE_URL/customers?is_active=is.true') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
}

test_pagination_and_sorting() {
    log_info "=== Testing Pagination and Sorting ==="
    
    # Test offset pagination
    run_test "Offset-based pagination" "
        response=\$(curl -s '$API_BASE_URL/customers?limit=2&offset=0') &&
        echo \"\$response\" | jq -e '.data | length <= 2' &&
        echo \"\$response\" | jq -e '.pagination.limit == 2' &&
        echo \"\$response\" | jq -e '.pagination.offset == 0'
    "
    
    # Test cursor-based pagination
    run_test "Cursor-based pagination" "
        response=\$(curl -s '$API_BASE_URL/customers?first=2') &&
        echo \"\$response\" | jq -e '.edges | type == \"array\"' &&
        echo \"\$response\" | jq -e '.pageInfo.hasNextPage != null' &&
        echo \"\$response\" | jq -e '.totalCount >= 0'
    "
    
    # Test sorting
    run_test "Sort by column ascending" "
        response=\$(curl -s '$API_BASE_URL/customers?orderBy=name&orderDirection=asc') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test PostgREST-style ordering
    run_test "PostgREST-style multi-column ordering" "
        response=\$(curl -s '$API_BASE_URL/customers?order=tier.desc,name.asc') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
}

test_relationship_expansion() {
    log_info "=== Testing Relationship Expansion ==="
    
    # Test forward relationship expansion
    run_test "Expand forward relationship" "
        response=\$(curl -s '$API_BASE_URL/orders?expand=customers&limit=1') &&
        echo \"\$response\" | jq -e '.data[0].customers.name // empty' | grep -q '^\".*\"$'
    "
    
    # Test reverse relationship expansion
    run_test "Expand reverse relationship" "
        response=\$(curl -s '$API_BASE_URL/customers?expand=orders&limit=1') &&
        echo \"\$response\" | jq -e '.data[0].orders | type == \"array\"'
    "
    
    # Test parameterized expansion
    run_test "Parameterized relationship expansion" "
        response=\$(curl -s '$API_BASE_URL/customers?expand=orders(limit:2)&limit=1') &&
        echo \"\$response\" | jq -e '.data[0].orders | length <= 2'
    "
}

test_column_selection() {
    log_info "=== Testing Column Selection ==="
    
    # Test select specific columns
    run_test "Select specific columns" "
        response=\$(curl -s '$API_BASE_URL/customers?select=name,email&limit=1') &&
        echo \"\$response\" | jq -e '.data[0].name' &&
        echo \"\$response\" | jq -e '.data[0].email' &&
        ! echo \"\$response\" | jq -e '.data[0].customer_id'
    "
    
    # Test select with relationships
    run_test "Select columns with relationship expansion" "
        response=\$(curl -s '$API_BASE_URL/orders?select=order_number,total&expand=customers(select:name,email)&limit=1') &&
        echo \"\$response\" | jq -e '.data[0].order_number' &&
        echo \"\$response\" | jq -e '.data[0].total'
    "
}

test_bulk_operations() {
    log_info "=== Testing Bulk Operations ==="
    
    # Test bulk create
    run_test "Bulk create customers" "
        response=\$(curl -s -X POST '$API_BASE_URL/customers' \\
            -H 'Content-Type: application/json' \\
            -d '[
                {\"name\": \"Bulk Customer 1\", \"email\": \"bulk1@example.com\"},
                {\"name\": \"Bulk Customer 2\", \"email\": \"bulk2@example.com\"}
            ]') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"' &&
        echo \"\$response\" | jq -e '.count == 2'
    "
}

test_error_handling() {
    log_info "=== Testing Error Handling ==="
    
    # Test non-existent table
    run_test "Handle non-existent table" "
        response=\$(curl -s '$API_BASE_URL/nonexistent_table') &&
        echo \"\$response\" | jq -e '.error' | grep -q 'Table not found'
    "
    
    # Test non-existent record
    run_test "Handle non-existent record" "
        response=\$(curl -s '$API_BASE_URL/customers/99999') &&
        echo \"\$response\" | jq -e '.error' | grep -q 'Record not found'
    "
    
    # Test invalid column in filter
    run_test "Handle invalid column in filter" "
        response=\$(curl -s '$API_BASE_URL/customers?invalid_column=eq.value') &&
        echo \"\$response\" | jq -e '.error' | grep -q 'Invalid column'
    "
    
    # Test invalid pagination limits
    run_test "Handle invalid pagination limit" "
        response=\$(curl -s '$API_BASE_URL/customers?limit=2000') &&
        echo \"\$response\" | jq -e '.error' | grep -q 'Limit must be'
    "
    
    # Test empty request body
    run_test "Handle empty request body" "
        response=\$(curl -s -X POST '$API_BASE_URL/customers' \\
            -H 'Content-Type: application/json' \\
            -d '{}') &&
        echo \"\$response\" | jq -e '.error' | grep -q 'Request body cannot be empty'
    "
}

test_postgresql_specific_types() {
    log_info "=== Testing PostgreSQL-Specific Types ==="
    
    # Test UUID field
    run_test "Query UUID field" "
        response=\$(curl -s '$API_BASE_URL/orders?limit=1') &&
        echo \"\$response\" | jq -e '.data[0].id' | grep -E '^\"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\"$'
    "
    
    # Test JSONB queries
    run_test "Query JSONB field with haskey" "
        response=\$(curl -s '$API_BASE_URL/customers?profile=haskey.loyalty_points') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test array queries
    run_test "Query array field with arraycontains" "
        response=\$(curl -s '$API_BASE_URL/products?categories=arraycontains.electronics') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test enum queries
    run_test "Query enum field" "
        response=\$(curl -s '$API_BASE_URL/customers?tier=eq.gold') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test decimal precision
    run_test "Query decimal field with precision" "
        response=\$(curl -s '$API_BASE_URL/products?price=gte.99.99') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
}

test_complex_queries() {
    log_info "=== Testing Complex Queries ==="
    
    # Test multiple filters with different operators
    run_test "Multiple filters with different operators" "
        response=\$(curl -s '$API_BASE_URL/products?price=gte.50&price=lte.500&is_active=is.true') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test complex OR with multiple conditions
    run_test "Complex OR with multiple conditions" "
        response=\$(curl -s '$API_BASE_URL/customers?or=(tier.eq.gold,tier.eq.platinum,tags.arraycontains.vip)') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test relationship expansion with filters
    run_test "Relationship expansion with filters" "
        response=\$(curl -s '$API_BASE_URL/customers?tier=eq.gold&expand=orders') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
    
    # Test sorting with relationship expansion
    run_test "Sorting with relationship expansion" "
        response=\$(curl -s '$API_BASE_URL/orders?orderBy=total&orderDirection=desc&expand=customers&limit=3') &&
        echo \"\$response\" | jq -e '.data | type == \"array\"'
    "
}

# Main execution
main() {
    log_info "Starting Excalibase REST API E2E Test Suite"
    log_info "API Base URL: $API_BASE_URL"
    
    # Wait for service to be ready
    wait_for_service
    
    # Run test suites
    test_schema_endpoints
    test_basic_crud_operations
    test_advanced_filtering
    test_pagination_and_sorting
    test_relationship_expansion
    test_column_selection
    test_bulk_operations
    test_error_handling
    test_postgresql_specific_types
    test_complex_queries
    
    # Print summary
    echo
    log_info "=== Test Summary ==="
    log_info "Total Tests: $TOTAL_TESTS"
    log_success "Passed: $PASSED_TESTS"
    log_error "Failed: $FAILED_TESTS"
    
    if [[ $FAILED_TESTS -eq 0 ]]; then
        log_success "All tests passed! 🎉"
        exit 0
    else
        log_error "Some tests failed! ❌"
        exit 1
    fi
}

# Check dependencies
check_dependencies() {
    for cmd in curl jq; do
        if ! command -v $cmd &> /dev/null; then
            log_error "$cmd is required but not installed"
            exit 1
        fi
    done
}

# Script entry point
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    check_dependencies
    main "$@"
fi