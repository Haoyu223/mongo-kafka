package com.mongodb.kafka.connect.util.resource;

import java.util.LinkedHashMap;
import java.util.Map;

public class ResourceFieldMetaDTO {

  private String fieldName;

  private Map<String, String> fieldDesc = new LinkedHashMap<>();

  private Boolean required;

  private DataType dataType;

  private ResourceFieldMetaIndex index;

  private DataType nestedArrayType;

  public void setIndex(ResourceFieldMetaIndex index) {
    this.index = index;
  }

  public ResourceFieldMetaIndex getIndex() {
    return index;
  }

  public DataType getNestedArrayType() {
    return nestedArrayType;
  }

  public void setNestedArrayType(DataType nestedArrayType) {
    this.nestedArrayType = nestedArrayType;
  }

  public Boolean getRequired() {
    return required;
  }

  public DataType getDataType() {
    return dataType;
  }

  public Map<String, String> getFieldDesc() {
    return fieldDesc;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public void setFieldDesc(Map<String, String> fieldDesc) {
    this.fieldDesc = fieldDesc;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public void setRequired(Boolean required) {
    this.required = required;
  }
}
