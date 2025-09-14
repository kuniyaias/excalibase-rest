package io.github.excalibase.service

import io.github.excalibase.model.SelectField
import io.github.excalibase.postgres.service.SelectParserService
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import spock.lang.Specification

class SelectParserServiceTest extends Specification {

    SelectParserService selectParserService

    def setup() {
        selectParserService = new SelectParserService()
    }

    def "should parse simple select fields"() {
        when:
        def fields = selectParserService.parseSelect("name,age,email")

        then:
        fields.size() == 3
        fields[0].name == "name"
        fields[0].isSimpleColumn()
        fields[1].name == "age"
        fields[1].isSimpleColumn()
        fields[2].name == "email"
        fields[2].isSimpleColumn()
    }

    def "should parse wildcard select"() {
        when:
        def fields = selectParserService.parseSelect("*")

        then:
        fields.size() == 1
        fields[0].name == "*"
        fields[0].isWildcard()
        !fields[0].isSimpleColumn()
    }

    def "should parse embedded fields"() {
        when:
        def fields = selectParserService.parseSelect("title,actors(name,age)")

        then:
        fields.size() == 2
        fields[0].name == "title"
        fields[0].isSimpleColumn()
        
        fields[1].name == "actors"
        fields[1].isEmbedded()
        fields[1].subFields.size() == 2
        fields[1].subFields[0].name == "name"
        fields[1].subFields[1].name == "age"
    }

    def "should parse complex nested embedded fields"() {
        when:
        def fields = selectParserService.parseSelect("name,posts(title,content)")

        then:
        fields.size() == 2
        fields[0].name == "name"
        
        fields[1].name == "posts"
        fields[1].isEmbedded()
        fields[1].subFields.size() == 2
        fields[1].subFields[0].name == "title"
        fields[1].subFields[1].name == "content"
    }

    def "should parse embedded fields with wildcards"() {
        when:
        def fields = selectParserService.parseSelect("*,actors(*)")

        then:
        fields.size() == 2
        fields[0].name == "*"
        fields[0].isWildcard()
        
        fields[1].name == "actors"
        fields[1].isEmbedded()
        fields[1].subFields.size() == 1
        fields[1].subFields[0].name == "*"
        fields[1].subFields[0].isWildcard()
    }

    def "should handle empty select parameter"() {
        when:
        def fields = selectParserService.parseSelect("")

        then:
        fields.size() == 1
        fields[0].name == "*"
        fields[0].isWildcard()
    }

    def "should handle null select parameter"() {
        when:
        def fields = selectParserService.parseSelect(null)

        then:
        fields.size() == 1
        fields[0].name == "*"
        fields[0].isWildcard()
    }

    def "should parse embedded filters from query parameters"() {
        given:
        def fields = selectParserService.parseSelect("title,actors(name,age)")
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>()
        params.add("actors.age", "gt.30")
        params.add("actors.nationality", "eq.American")
        params.add("title", "like.Matrix")

        when:
        selectParserService.parseEmbeddedFilters(fields, params)

        then:
        def actorsField = fields.find { it.name == "actors" }
        actorsField != null
        actorsField.filters.size() == 2
        actorsField.filters["age"] == "gt.30"
        actorsField.filters["nationality"] == "eq.American"
        
        // Regular filters should not be added to embedded fields
        def titleField = fields.find { it.name == "title" }
        titleField.filters.isEmpty()
    }

    def "should identify simple column names correctly"() {
        when:
        def fields = selectParserService.parseSelect("name,age,actors(first_name)")
        def simpleColumns = selectParserService.getSimpleColumnNames(fields)

        then:
        simpleColumns.size() == 2
        simpleColumns.contains("name")
        simpleColumns.contains("age")
    }

    def "should identify embedded fields correctly"() {
        when:
        def fields = selectParserService.parseSelect("name,actors(first_name),posts(title)")
        def embeddedFields = selectParserService.getEmbeddedFields(fields)

        then:
        embeddedFields.size() == 2
        embeddedFields[0].name == "actors"
        embeddedFields[1].name == "posts"
    }

    def "should detect embedded fields presence"() {
        expect:
        selectParserService.hasEmbeddedFields(selectParserService.parseSelect("name,age")) == false
        selectParserService.hasEmbeddedFields(selectParserService.parseSelect("name,actors(age)")) == true
    }

    def "should handle malformed embedded syntax gracefully"() {
        when:
        def fields = selectParserService.parseSelect("name,actors(incomplete")

        then:
        fields.size() == 2
        fields[0].name == "name"
        fields[1].name == "actors(incomplete"
        !fields[1].isEmbedded()
    }

    def "should parse mixed simple and embedded fields with complex nesting"() {
        when:
        def fields = selectParserService.parseSelect("id,title,actors(name,age),reviews(*)")

        then:
        fields.size() == 4
        
        // Simple fields
        fields[0].name == "id"
        fields[1].name == "title"
        
        // Embedded field
        def actorsField = fields[2]
        actorsField.name == "actors"
        actorsField.isEmbedded()
        actorsField.subFields.size() == 2
        actorsField.subFields[0].name == "name"
        actorsField.subFields[1].name == "age"
        
        // Embedded field with wildcard
        def reviewsField = fields[3]
        reviewsField.name == "reviews"
        reviewsField.isEmbedded()
        reviewsField.subFields.size() == 1
        reviewsField.subFields[0].isWildcard()
    }
}