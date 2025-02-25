package com.mongodb.kafka.connect.sink.cdc.debezium;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class DdlOperation implements CdcOperation {

  public DdlOperation() {}

  @Override
  public WriteModel<BsonDocument> perform(SinkDocument doc) {
    BsonDocument value =
        doc.getValueDoc().orElseThrow(() -> new DataException("Missing DDL value document"));

    //        BsonDocument ddlDoc = new BsonDocument();
    //        // 从value中提取必要字段
    //        if (value.containsKey("databaseName")) {
    //            ddlDoc.put("database", value.get("databaseName"));
    //        }
    //        ddlDoc.put("ddl", value.get("ddl"));
    //        ddlDoc.put("ts_ms", value.get("ts_ms"));

    return new InsertOneModel<>(value);
  }
}
