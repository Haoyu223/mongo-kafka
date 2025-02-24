/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTask.LOGGER;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.BULK_WRITE_ORDERED_CONFIG;
import static com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.dlq.AnalyzedBatchFailedWithBulkWriteException;
import com.mongodb.kafka.connect.sink.dlq.ErrorReporter;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;
import com.mongodb.kafka.connect.util.jmx.SinkTaskStatistics;
import com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils;
import com.mongodb.kafka.connect.util.resource.*;
import com.mongodb.kafka.connect.util.time.InnerOuterTimer;
import com.mongodb.kafka.connect.util.time.InnerOuterTimer.InnerTimer;
import com.mongodb.kafka.connect.util.time.Timer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;

final class StartedMongoSinkTask implements AutoCloseable {
  private final MongoSinkConfig sinkConfig;
  private final MongoClient mongoClient;
  private final ErrorReporter errorReporter;
  private final Set<MongoNamespace> checkedTimeseriesNamespaces;

  private final SinkTaskStatistics statistics;
  private final InnerOuterTimer inTaskPutInConnectFrameworkTimer;

  StartedMongoSinkTask(
      final MongoSinkConfig sinkConfig,
      final MongoClient mongoClient,
      final ErrorReporter errorReporter) {
    this.sinkConfig = sinkConfig;
    this.mongoClient = mongoClient;
    this.errorReporter = errorReporter;
    checkedTimeseriesNamespaces = new HashSet<>();
    statistics = new SinkTaskStatistics(getMBeanName());
    statistics.register();
    inTaskPutInConnectFrameworkTimer =
        InnerOuterTimer.start(
            (inTaskPutSample) -> {
              statistics.getInTaskPut().sample(inTaskPutSample.toMillis());
              if (LOGGER.isDebugEnabled()) {
                // toJSON relatively expensive
                LOGGER.debug(statistics.getName() + ": " + statistics.toJSON());
              }
            },
            (inFrameworkSample) ->
                statistics.getInConnectFramework().sample(inFrameworkSample.toMillis()));
  }

  private String getMBeanName() {
    String id = MBeanServerUtils.taskIdFromCurrentThread();
    String connectorName = JmxStatisticsManager.getConnectorName(this.sinkConfig.getOriginals());
    return "com.mongodb.kafka.connect:type=sink-task-metrics,connector="
        + connectorName
        + ",task=sink-task-"
        + id;
  }

  /** @see MongoSinkTask#stop() */
  @SuppressWarnings("try")
  @Override
  public void close() {
    try (MongoClient autoCloseable = mongoClient) {
      statistics.unregister();
    }
  }

  /** @see MongoSinkTask#put(Collection) */
  @SuppressWarnings("try")
  void put(final Collection<SinkRecord> records) {
    try (InnerTimer automatic = inTaskPutInConnectFrameworkTimer.sampleOuter()) {
      statistics.getRecords().sample(records.size());
      trackLatestRecordTimestampOffset(records);
      if (records.isEmpty()) {
        LOGGER.info("No sink records to process for current poll operation");
      } else {
        Timer processingTime = Timer.start();
        List<List<MongoProcessedSinkRecordData>> batches =
            MongoSinkRecordProcessor.orderedGroupByTopicAndNamespace(
                records, sinkConfig, errorReporter);
        statistics.getProcessingPhases().sample(processingTime.getElapsedTime().toMillis());
        for (List<MongoProcessedSinkRecordData> batch : batches) {
          processDdl(batch);
        }
      }
    }
  }

  private void trackLatestRecordTimestampOffset(final Collection<SinkRecord> records) {
    OptionalLong latestRecord =
        records.stream()
            .filter(v -> v.timestamp() != null)
            .mapToLong(ConnectRecord::timestamp)
            .max();
    if (latestRecord.isPresent()) {
      long offsetMs = System.currentTimeMillis() - latestRecord.getAsLong();
      statistics.getLatestKafkaTimeDifferenceMs().sample(offsetMs);
    }
  }

  private void processDdl(final List<MongoProcessedSinkRecordData> batch) {
    if (batch.isEmpty()) {
      return;
    }

    for (final MongoProcessedSinkRecordData recordData : batch) {
      MongoSinkTopicConfig config = recordData.getConfig();

      SinkRecord record = recordData.getSinkRecord();
      String ddl = objectToDdlString(record.value());

      if (null != ddl) {
        LOGGER.info("parsing ddl statement: {}", ddl);
        parseDdl(ddl);
      } else {
        LOGGER.info(
            "The ddl is null, or transfer failed, topic:{}, partition:{}, offset:{}",
            record.topic(),
            record.kafkaPartition(),
            record.kafkaOffset());
      }

      checkRateLimit(config);
    }
  }

  private void parseDdl(final String ddl) {
    try {
      //      MongoNamespace namespace = recordData.getNamespace();

      Statement statement = CCJSqlParserUtil.parse(ddl);

      if (statement instanceof CreateTable) {
        processCreateTable((CreateTable) statement);
      } else if (statement instanceof Alter) {
        processAlter((Alter) statement);
      } else if (statement instanceof Drop) {
        processDrop((Drop) statement);
      }

    } catch (Exception e) {
      LOGGER.warn("error parsing DDL: {}, message: {}", ddl, e.getMessage());
    }
  }

  private void processCreateTable(final CreateTable createTable) {
    // could deal with createTable.isIfNotExists()
    String table = createTable.getTable().getName();
    if (CollectionUtils.isNotEmpty(createTable.getColumnDefinitions())) {
      Map<String, String> contentMap =
          createTable.getColumnDefinitions().stream()
              .collect(
                  Collectors.toMap(
                      ColumnDefinition::getColumnName,
                      cd -> cd.getColDataType().getDataType(),
                      (existing, replacement) -> existing,
                      HashMap::new));

      // try to create collection first
      // this step may throw error, since the table may exist
      mongoClient.getDatabase(MongoResourceConstant.TEST_DATABASE).createCollection(table);
      LOGGER.info("created table success: {}", table);

      // check schema meta exist or not
      Bson query =
          Filters.and(
              Filters.eq(MongoResourceConstant.RESOURCE_NAME, table),
              Filters.eq(MongoResourceConstant.TENANT_ID, MongoResourceConstant.DEMO_TENANTID));
      Document result =
          mongoClient
              .getDatabase(MongoResourceConstant.TEST_DATABASE)
              .getCollection(MongoResourceConstant.RESOURCE_META)
              .find(query)
              .first();
      if (result == null) {
        // add a message to resource_meta
        CreateResourceSchemaVO schemaVO = prepareResourceSchema(table, contentMap);
        Document document = ResourceMetaUtil.generateDocumentFromResourceMetaDTO(schemaVO);

        // todo need to change database
        mongoClient
            .getDatabase(MongoResourceConstant.TEST_DATABASE)
            .getCollection(MongoResourceConstant.RESOURCE_META)
            .insertOne(document);
        LOGGER.info("Inserted document to mongo success: {}", document.toJson());
      } else {
        LOGGER.info(
            "resource_meta has exist record, create operation paused. table: {}, tenant: {}, message: {}",
            table,
            MongoResourceConstant.DEMO_TENANTID,
            result.toJson());
      }
    }
  }

  private void processAlter(final Alter alter) {
    String table = alter.getTable().getName();
    for (AlterExpression expression : alter.getAlterExpressions()) {
      if (CollectionUtils.isNotEmpty(expression.getColDataTypeList())) {

        // check schema meta exist or not
        Bson query =
            Filters.and(
                Filters.eq(MongoResourceConstant.RESOURCE_NAME, table),
                Filters.eq(MongoResourceConstant.TENANT_ID, MongoResourceConstant.DEMO_TENANTID));
        Document result =
            mongoClient
                .getDatabase(MongoResourceConstant.TEST_DATABASE)
                .getCollection(MongoResourceConstant.RESOURCE_META)
                .find(query)
                .first();

        if (result != null) {
          // todo might need to change DTO
          List<ResourceFieldMetaDTO> resourceFieldMetaList =
              result.getList(MongoResourceConstant.RESOURCE_FIELD_LIST, ResourceFieldMetaDTO.class);

          String operation = expression.getOperation().toString();
          Map<String, String> contentMap =
              expression.getColDataTypeList().stream()
                  .collect(
                      Collectors.toMap(
                          AlterExpression.ColumnDataType::getColumnName,
                          cd -> cd.getColDataType().getDataType(),
                          (existing, replacement) -> existing,
                          HashMap::new));

          List<ResourceFieldMetaDTO> dtos =
              updateResourceFieldMetaDTOS(resourceFieldMetaList, contentMap, operation);
          result.put(MongoResourceConstant.RESOURCE_FIELD_LIST, dtos);

          mongoClient
              .getDatabase(MongoResourceConstant.TEST_DATABASE)
              .getCollection(MongoResourceConstant.RESOURCE_META)
              .replaceOne(
                  Filters.eq(
                      MongoResourceConstant.RESERVED_FIELD_UNDERSCORE_ID,
                      result.get(MongoResourceConstant.RESERVED_FIELD_UNDERSCORE_ID)),
                  result);

          LOGGER.info("Altered resource fields success: {}", result.toJson());
        }
      }
    }
  }

  private void processDrop(final Drop drop) {
    if (drop.getType() != null && drop.getType().equalsIgnoreCase(MongoResourceConstant.TABLE)) {
      String table = drop.getName().getName();
      // todo need to change database
      mongoClient.getDatabase(MongoResourceConstant.TEST_DATABASE).getCollection(table).drop();
      LOGGER.info("dropped table success: {}", table);
    }
  }

  private CreateResourceSchemaVO prepareResourceSchema(
      String table, Map<String, String> contentMap) {
    CreateResourceSchemaVO createResourceSchemaVO = new CreateResourceSchemaVO();
    // todo fix demo
    createResourceSchemaVO.setTenantId(MongoResourceConstant.DEMO_TENANTID);
    createResourceSchemaVO.setResourceName(table);

    Map<String, String> resourceDisplayName = new LinkedHashMap<>();
    resourceDisplayName.put(Language.en.name(), table);
    createResourceSchemaVO.setResourceDisplayName(resourceDisplayName);

    List<ResourceFieldMetaDTO> resourceFieldMetaList = getResourceFieldMetaDTOS(contentMap);

    createResourceSchemaVO.setResourceFieldMetaList(resourceFieldMetaList);
    return createResourceSchemaVO;
  }

  private List<ResourceFieldMetaDTO> getResourceFieldMetaDTOS(Map<String, String> contentMap) {
    List<ResourceFieldMetaDTO> resourceFieldMetaList = new ArrayList<>();
    for (String column : contentMap.keySet()) {
      ResourceFieldMetaDTO fieldMetaDTO = new ResourceFieldMetaDTO();
      fieldMetaDTO.setFieldName(column);
      Map<String, String> fieldDesc = new LinkedHashMap<>();
      fieldDesc.put(Language.en.name(), column);
      fieldMetaDTO.setFieldDesc(fieldDesc);
      fieldMetaDTO.setRequired(true);
      // todo extend
      fieldMetaDTO.setDataType(
          (contentMap.get(column).contains("TIME")
                  || contentMap.get(column).contains("DATE")
                  || contentMap.get(column).contains("YEAR"))
              ? DataType.DATE
              : DataType.STRING);
      resourceFieldMetaList.add(fieldMetaDTO);
    }
    return resourceFieldMetaList;
  }

  private List<ResourceFieldMetaDTO> updateResourceFieldMetaDTOS(
      List<ResourceFieldMetaDTO> resourceFieldMetaDTOS,
      Map<String, String> contentMap,
      String operation) {
    List<ResourceFieldMetaDTO> result = new ArrayList<>(resourceFieldMetaDTOS);
    if (operation != null && operation.equalsIgnoreCase(MongoResourceConstant.ADD)) {
      for (String column : contentMap.keySet()) {
        ResourceFieldMetaDTO fieldMetaDTO = new ResourceFieldMetaDTO();
        fieldMetaDTO.setFieldName(column);
        Map<String, String> fieldDesc = new LinkedHashMap<>();
        fieldDesc.put(Language.en.name(), column);
        fieldMetaDTO.setFieldDesc(fieldDesc);
        fieldMetaDTO.setRequired(true);
        // todo extend
        fieldMetaDTO.setDataType(
            (contentMap.get(column).contains("TIME")
                    || contentMap.get(column).contains("DATE")
                    || contentMap.get(column).contains("YEAR"))
                ? DataType.DATE
                : DataType.STRING);
        result.add(fieldMetaDTO);
      }
    } else if (operation != null
        && operation.equalsIgnoreCase(MongoResourceConstant.DROP)
        && !contentMap.isEmpty()) {
      for (ResourceFieldMetaDTO dto : resourceFieldMetaDTOS) {
        String fieldName = dto.getFieldName();
        if (contentMap.containsKey(fieldName)) {
          result.remove(dto);
        }
      }
    }
    return result;
  }

  private String objectToDdlString(final Object object) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      String json = objectMapper.writeValueAsString(object);
      Map<String, Object> map =
          objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
      if (map.containsKey("ddl")) {
        Object ddlValue = map.get("ddl");
        if (ddlValue instanceof String) {
          String ddl = (String) ddlValue;
          return ddl.replaceAll("\n", " ");
        }
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  @Deprecated
  private void bulkWriteBatch(final List<MongoProcessedSinkRecordData> batch) {
    if (batch.isEmpty()) {
      return;
    }

    MongoNamespace namespace = batch.get(0).getNamespace();
    MongoSinkTopicConfig config = batch.get(0).getConfig();
    checkTimeseries(namespace, config);

    List<WriteModel<BsonDocument>> writeModels =
        batch.stream()
            .map(MongoProcessedSinkRecordData::getWriteModel)
            .collect(Collectors.toList());
    boolean bulkWriteOrdered = config.getBoolean(BULK_WRITE_ORDERED_CONFIG);

    Timer writeTime = Timer.start();
    try {
      LOGGER.debug(
          "Bulk writing {} document(s) into collection [{}] via an {} bulk write",
          writeModels.size(),
          namespace.getFullName(),
          bulkWriteOrdered ? "ordered" : "unordered");
      BulkWriteResult result =
          mongoClient
              .getDatabase(namespace.getDatabaseName())
              .getCollection(namespace.getCollectionName(), BsonDocument.class)
              .bulkWrite(writeModels, new BulkWriteOptions().ordered(bulkWriteOrdered));
      statistics.getBatchWritesSuccessful().sample(writeTime.getElapsedTime().toMillis());
      statistics.getRecordsSuccessful().sample(batch.size());
      LOGGER.debug("Mongodb bulk write result: {}", result);
    } catch (RuntimeException e) {
      statistics.getBatchWritesFailed().sample(writeTime.getElapsedTime().toMillis());
      statistics.getRecordsFailed().sample(batch.size());
      if (config.tolerateDataErrors() && !(e instanceof MongoBulkWriteException)) {
        throw new DataException("non Data Error, fail the connector.", e);
      }
      handleTolerableWriteException(
          batch.stream()
              .map(MongoProcessedSinkRecordData::getSinkRecord)
              .collect(Collectors.toList()),
          bulkWriteOrdered,
          e,
          config.logErrors(),
          config.tolerateErrors() || config.tolerateDataErrors());
    }
    checkRateLimit(config);
  }

  private void checkTimeseries(final MongoNamespace namespace, final MongoSinkTopicConfig config) {
    if (!checkedTimeseriesNamespaces.contains(namespace)) {
      if (config.isTimeseries()) {
        validateCollection(mongoClient, namespace, config);
      }
      checkedTimeseriesNamespaces.add(namespace);
    }
  }

  private static void checkRateLimit(final MongoSinkTopicConfig config) {
    RateLimitSettings rls = config.getRateLimitSettings();
    if (rls.isTriggered()) {
      LOGGER.debug(
          "Rate limit settings triggering {}ms defer timeout after processing {} further batches for topic {}",
          rls.getTimeoutMs(),
          rls.getEveryN(),
          config.getTopic());
      try {
        Thread.sleep(rls.getTimeoutMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DataException("Rate limiting was interrupted", e);
      }
    }
  }

  private void handleTolerableWriteException(
      final List<SinkRecord> batch,
      final boolean ordered,
      final RuntimeException e,
      final boolean logErrors,
      final boolean tolerateErrors) {
    if (e instanceof MongoBulkWriteException) {
      AnalyzedBatchFailedWithBulkWriteException analyzedBatch =
          new AnalyzedBatchFailedWithBulkWriteException(
              batch,
              ordered,
              (MongoBulkWriteException) e,
              errorReporter,
              StartedMongoSinkTask::log);
      if (logErrors) {
        LOGGER.error(
            "Failed to put into the sink some records, see log entries below for the details", e);
        analyzedBatch.log();
      }
      if (tolerateErrors) {
        analyzedBatch.report();
      } else {
        throw new DataException(e);
      }
    } else {
      if (logErrors) {
        log(batch, e);
      }
      if (tolerateErrors) {
        batch.forEach(record -> errorReporter.report(record, e));
      } else {
        throw new DataException(e);
      }
    }
  }

  private static void log(final Collection<SinkRecord> records, final RuntimeException e) {
    LOGGER.error("Failed to put into the sink the following records: {}", records, e);
  }
}
