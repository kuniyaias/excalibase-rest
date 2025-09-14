package io.github.excalibase.model;

import java.util.List;
import java.util.Objects;

/**
 * Represents information about a custom enum type in the database.
 */
public class CustomEnumTypeInfo {
    private String schema;
    private String name;
    private List<String> values;

    public CustomEnumTypeInfo() {
    }

    public CustomEnumTypeInfo(String schema, String name, List<String> values) {
        this.schema = schema;
        this.name = name;
        this.values = values;
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

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomEnumTypeInfo that = (CustomEnumTypeInfo) o;
        return Objects.equals(schema, that.schema) &&
               Objects.equals(name, that.name) &&
               Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, values);
    }

    @Override
    public String toString() {
        return "CustomEnumTypeInfo{" +
               "schema='" + schema + '\'' +
               ", name='" + name + '\'' +
               ", values=" + values +
               '}';
    }
}