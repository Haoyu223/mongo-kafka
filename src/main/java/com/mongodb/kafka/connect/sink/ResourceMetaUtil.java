package com.mongodb.kafka.connect.sink;

import com.mongodb.kafka.connect.util.resource.*;
import org.bson.Document;

import java.util.*;

final class ResourceMetaUtil {

    static Document generateDocumentFromResourceMetaDTO(final CreateResourceSchemaVO schemaVO){
        Document document = new Document()
                .append(MongoResourceConstant.TENANT_ID, schemaVO.getTenantId())
                .append(MongoResourceConstant.VERSION, schemaVO.getVersion())
                .append(MongoResourceConstant.RESOURCE_NAME, schemaVO.getResourceName());

        Document displayNameDocument = new Document();
        for (Map.Entry<String, String> resourceMetaDisplayNameEntry : schemaVO.getResourceDisplayName().entrySet()) {
            displayNameDocument = displayNameDocument.append(resourceMetaDisplayNameEntry.getKey(), resourceMetaDisplayNameEntry.getValue());
        }
        document = document.append(MongoResourceConstant.RESOURCE_DISPLAY_NAME, displayNameDocument)
                .append(MongoResourceConstant.RESERVED, schemaVO.getReserved());

        Date date = new Date();
        document = document.append(MongoResourceConstant.RESERVED_FIELD_CREATED_BY, MongoResourceConstant.SINK_CONNECTOR_ID);
        document = document.append(MongoResourceConstant.RESERVED_FIELD_CREATED_DATE, date);
        document = document.append(MongoResourceConstant.RESERVED_FIELD_UPDATED_BY, MongoResourceConstant.SINK_CONNECTOR_ID);
        document = document.append(MongoResourceConstant.RESERVED_FIELD_UPDATED_DATE, date);

        // add reserved field definition
        List<Document> resourceFieldDocList = new ArrayList<>();
        schemaVO.getResourceFieldMetaList().add(0, getPredefinedFieldMetaForID());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForTags());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForVersion());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForImportId());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForCreatedBy());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForCreatedDate());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForUpdatedBy());
        schemaVO.getResourceFieldMetaList().add(getPredefinedFieldMetaForUpdatedDate());

        schemaVO.getResourceFieldMetaList().forEach(item-> resourceFieldDocList.add(generateDocumentFromResourceFieldMetaDTO(item)));

        document.put(MongoResourceConstant.RESOURCE_FIELD_LIST, resourceFieldDocList);

        //other logic
//        document.put(MongoResourceConstant.RESOURCE_META_BYTE_STREAM, new Binary(SerializationUtils.serialize(schemaVO)));
//        document.put(MongoResourceConstant.RESOURCE_META_MAP_BYTE_STREAM, new Binary(SerializationUtils.serialize(resourceMetaMap)));
        //todo need to change
        document.put(MongoResourceConstant.RESOURCE_META_BYTE_STREAM, null);
        document.put(MongoResourceConstant.RESOURCE_META_MAP_BYTE_STREAM, null);

        return document;
    }

    private static Document generateDocumentFromResourceFieldMetaDTO(ResourceFieldMetaDTO resourceFieldMetaDTO){
        Document document = new Document()
                .append(MongoResourceConstant.FIELD_NAME, resourceFieldMetaDTO.getFieldName());

        Document fieldDescDocument = new Document();
        for (Map.Entry<String, String> entry : resourceFieldMetaDTO.getFieldDesc().entrySet()) {
            fieldDescDocument = fieldDescDocument.append(entry.getKey(), entry.getValue());
        }

        document = document
                .append(MongoResourceConstant.FIELD_DESC, fieldDescDocument)
                .append(MongoResourceConstant.FIELD_REQUIRED, resourceFieldMetaDTO.getRequired())
                .append(MongoResourceConstant.DATA_TYPE, resourceFieldMetaDTO.getDataType().name());

        // handle index object
        if (resourceFieldMetaDTO.getIndex() != null) {
            Document indexDocument = new Document();
            if (resourceFieldMetaDTO.getIndex().getName() != null) {
                indexDocument.put(ResourceFieldMetaIndex.INDEX_NAME, resourceFieldMetaDTO.getIndex().getName());
            }
            if (resourceFieldMetaDTO.getIndex().getUnique() != null) {
                indexDocument.put(ResourceFieldMetaIndex.INDEX_UNIQUE, resourceFieldMetaDTO.getIndex().getUnique());
            }
            if (resourceFieldMetaDTO.getIndex().getOrder() != null) {
                indexDocument.put(ResourceFieldMetaIndex.INDEX_ORDER, resourceFieldMetaDTO.getIndex().getOrder().name());
            }
            if (resourceFieldMetaDTO.getIndex().getText() != null) {
                indexDocument.put(ResourceFieldMetaIndex.INDEX_TEXT, resourceFieldMetaDTO.getIndex().getText());
            }
            document.put(MongoResourceConstant.INDEX, indexDocument);
        }

        // handle array
        if (resourceFieldMetaDTO.getDataType() == DataType.ARRAY) {
            document.put(MongoResourceConstant.NESTED_ARRAY_TYPE, resourceFieldMetaDTO.getNestedArrayType().name());
        }
        
        //todo handle other

        return document;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForID() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_UNDERSCORE_ID);
        resourceFieldMetaDTO.setDataType(DataType.OBJECT_ID);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Document ID");
        descMap.put(Language.cn.name(), "文档ID");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForTags() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_TAGS);
        resourceFieldMetaDTO.setDataType(DataType.ARRAY);
        ResourceFieldMetaIndex resourceFieldMetaIndexDTO = new ResourceFieldMetaIndex(null, false, false, ResourceFieldMetaIndex.IndexOrder.DESCENDING);
        resourceFieldMetaDTO.setIndex(resourceFieldMetaIndexDTO);
        resourceFieldMetaDTO.setNestedArrayType(DataType.INTEGER);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Tags");
        descMap.put(Language.cn.name(), "标签");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForVersion() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.VERSION);
        resourceFieldMetaDTO.setDataType(DataType.INTEGER);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Resource Meta Version");
        descMap.put(Language.cn.name(), "元数据版本号");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForImportId() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_IMPORT_ID);
        resourceFieldMetaDTO.setDataType(DataType.OBJECT_ID);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Data Import ID");
        descMap.put(Language.cn.name(), "数据导入ID");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForCreatedBy() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_CREATED_BY);
        resourceFieldMetaDTO.setDataType(DataType.INTEGER);
        ResourceFieldMetaIndex resourceFieldMetaIndexDTO = new ResourceFieldMetaIndex(null, false, false, ResourceFieldMetaIndex.IndexOrder.ASCENDING);
        resourceFieldMetaDTO.setIndex(resourceFieldMetaIndexDTO);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Created By");
        descMap.put(Language.cn.name(), "创建人");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForCreatedDate() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_CREATED_DATE);
        resourceFieldMetaDTO.setDataType(DataType.DATE);
        ResourceFieldMetaIndex resourceFieldMetaIndexDTO = new ResourceFieldMetaIndex(null, false, false, ResourceFieldMetaIndex.IndexOrder.DESCENDING);
        resourceFieldMetaDTO.setIndex(resourceFieldMetaIndexDTO);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Created Date");
        descMap.put(Language.cn.name(), "创建时间");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForUpdatedBy() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_UPDATED_BY);
        resourceFieldMetaDTO.setDataType(DataType.INTEGER);
        ResourceFieldMetaIndex resourceFieldMetaIndexDTO = new ResourceFieldMetaIndex(null, false, false, ResourceFieldMetaIndex.IndexOrder.ASCENDING);
        resourceFieldMetaDTO.setIndex(resourceFieldMetaIndexDTO);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Updated By");
        descMap.put(Language.cn.name(), "更新人");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }

    private static ResourceFieldMetaDTO getPredefinedFieldMetaForUpdatedDate() {
        ResourceFieldMetaDTO resourceFieldMetaDTO = new ResourceFieldMetaDTO();
        resourceFieldMetaDTO.setFieldName(MongoResourceConstant.RESERVED_FIELD_UPDATED_DATE);
        resourceFieldMetaDTO.setDataType(DataType.DATE);
        ResourceFieldMetaIndex resourceFieldMetaIndexDTO = new ResourceFieldMetaIndex(null, false, false, ResourceFieldMetaIndex.IndexOrder.DESCENDING);
        resourceFieldMetaDTO.setIndex(resourceFieldMetaIndexDTO);
        Map<String, String> descMap = new LinkedHashMap<>();
        descMap.put(Language.en.name(), "Updated Date");
        descMap.put(Language.cn.name(), "更新时间");
        resourceFieldMetaDTO.setFieldDesc(descMap);
        return resourceFieldMetaDTO;
    }
}
