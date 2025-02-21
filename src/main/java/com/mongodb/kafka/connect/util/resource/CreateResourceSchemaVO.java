package com.mongodb.kafka.connect.util.resource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CreateResourceSchemaVO {

  private String tenantId;

  private Integer version = 1;

  private String resourceName;

  private Map<String, String> resourceDisplayName = new LinkedHashMap<>();

  private Boolean reserved = Boolean.FALSE;

  private List<ResourceFieldMetaDTO> resourceFieldMetaList;

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getTenantId() {
    return tenantId;
  }

  public Boolean getReserved() {
    return reserved;
  }

  public Integer getVersion() {
    return version;
  }

  public List<ResourceFieldMetaDTO> getResourceFieldMetaList() {
    return resourceFieldMetaList;
  }

  public String getResourceName() {
    return resourceName;
  }

  public Map<String, String> getResourceDisplayName() {
    return resourceDisplayName;
  }

  public void setReserved(Boolean reserved) {
    this.reserved = reserved;
  }

  public void setResourceDisplayName(Map<String, String> resourceDisplayName) {
    this.resourceDisplayName = resourceDisplayName;
  }

  public void setResourceFieldMetaList(List<ResourceFieldMetaDTO> resourceFieldMetaList) {
    this.resourceFieldMetaList = resourceFieldMetaList;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public void setVersion(Integer version) {
    this.version = version;
  }
}
