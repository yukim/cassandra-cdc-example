package com.datastax.oss.cdc.cassandra;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOError;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

public class JsonOutput {

    public static String toJson(ChangeEvent event) {
        StringWriter sw = new StringWriter();
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory jsonFactory = mapper.getFactory();
        try {
            JsonGenerator json = jsonFactory.createGenerator(sw);
            json.useDefaultPrettyPrinter();

            json.writeStartObject();

            // event type
            json.writeStringField("timestamp", event.getEventTimestamp().toString());
            // keyspace name
            json.writeStringField("keyspace", event.getKeyspaceName());
            json.writeStringField("table", event.getTableName());
            // table Id
            json.writeStringField("table_id", event.getTableId().toString());
            json.writeStringField("type", event.getEventType().toString().toLowerCase());

            if (event.getEventType() == ChangeEventType.UPDATE) {
                json.writeFieldName("row");
                json.writeStartObject();
                for (Map.Entry<String, Object> column : event.getRow().getColumns().entrySet()) {
                    json.writeFieldName(column.getKey());
                    json.writeObject(column.getValue());
                }
                json.writeEndObject();
            } else if (event.getEventType() == ChangeEventType.DELETE) {
                Deletion deletion = event.getDeletion();
                if (!deletion.getColumns().isEmpty()) {
                    json.writeFieldName("columns");
                    json.writeStartArray();
                    for (String col : deletion.getColumns()) {
                        json.writeString(col);
                    }
                    json.writeEndArray();
                }
                json.writeFieldName("criteria");
                json.writeStartObject();
                for (Criteria c : deletion.getCriteria()) {
                    json.writeFieldName(c.getColumn());
                    json.writeObject(c.getCondition());
                }
                json.writeEndObject();
            }

            json.writeEndObject();
            json.flush();
            return sw.toString();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
