package com.mongodb.kafka.connect.util.resource;

import java.io.Serializable;
import java.util.Objects;

public class ResourceFieldMetaIndex implements Serializable {

    public static final String INDEX_NAME = "name";

    public static final String INDEX_UNIQUE = "unique";

    public static final String INDEX_ORDER = "order";

    public static final String INDEX_TEXT = "text";

    public ResourceFieldMetaIndex() {

    }

    public ResourceFieldMetaIndex(String name, Boolean unique, Boolean text, IndexOrder order) {
        this.name = name;
        this.unique = unique;
        this.text = text;
        this.order = order;
    }

    public ResourceFieldMetaIndex(ResourceFieldMetaIndex resourceFieldMetaIndex) {
        this.name = resourceFieldMetaIndex.name;
        this.unique = resourceFieldMetaIndex.unique;
        this.order = resourceFieldMetaIndex.order;
        this.text = resourceFieldMetaIndex.text;
    }

    private String name;

    private Boolean unique = false;

    private Boolean text = false;

    private IndexOrder order = IndexOrder.ASCENDING;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getUnique() {
        return unique;
    }

    public void setUnique(Boolean unique) {
        if (unique == null) {
            this.unique = false;
        } else {
            this.unique = unique;
        }
    }

    public Boolean getText() {
        return text;
    }

    public void setText(Boolean text) {
        if (text == null) {
            this.text = false;
        } else {
            this.text = text;
        }
    }

    public IndexOrder getOrder() {
        return order;
    }

    public void setOrder(IndexOrder order) {
        if (order == null) {
            this.order = IndexOrder.ASCENDING;
        } else {
            this.order = order;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceFieldMetaIndex that = (ResourceFieldMetaIndex) o;
        return Objects.equals(name, that.name) && unique.equals(that.unique) && text.equals(that.text) && order == that.order;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, unique, text, order);
    }

    public static enum IndexOrder {
        ASCENDING, DESCENDING
    }
}
