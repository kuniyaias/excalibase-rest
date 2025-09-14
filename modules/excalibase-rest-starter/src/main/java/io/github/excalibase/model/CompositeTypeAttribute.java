package io.github.excalibase.model;

import java.util.Objects;

/**
 * Represents an attribute of a composite type in the database.
 */
public class CompositeTypeAttribute {
    private String name;
    private String type;
    private int order;

    public CompositeTypeAttribute() {
    }

    public CompositeTypeAttribute(String name, String type, int order) {
        this.name = name;
        this.type = type;
        this.order = order;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompositeTypeAttribute that = (CompositeTypeAttribute) o;
        return order == that.order &&
               Objects.equals(name, that.name) &&
               Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, order);
    }

    @Override
    public String toString() {
        return "CompositeTypeAttribute{" +
               "name='" + name + '\'' +
               ", type='" + type + '\'' +
               ", order=" + order +
               '}';
    }
}