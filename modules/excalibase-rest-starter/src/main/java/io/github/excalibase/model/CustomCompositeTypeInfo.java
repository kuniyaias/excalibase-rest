package io.github.excalibase.model;

import java.util.List;
import java.util.Objects;

/**
 * Represents information about a custom composite type in the database.
 */
public class CustomCompositeTypeInfo {
    private String schema;
    private String name;
    private List<CompositeTypeAttribute> attributes;

    public CustomCompositeTypeInfo() {
    }

    public CustomCompositeTypeInfo(String schema, String name, List<CompositeTypeAttribute> attributes) {
        this.schema = schema;
        this.name = name;
        this.attributes = attributes;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<CompositeTypeAttribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<CompositeTypeAttribute> attributes) {
        this.attributes = attributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomCompositeTypeInfo that = (CustomCompositeTypeInfo) o;
        return Objects.equals(schema, that.schema) &&
               Objects.equals(name, that.name) &&
               Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, attributes);
    }

    @Override
    public String toString() {
        return "CustomCompositeTypeInfo{" +
               "schema='" + schema + '\'' +
               ", name='" + name + '\'' +
               ", attributes=" + attributes +
               '}';
    }
}