/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 * Copyright © 2021 Ruslan Danilin (rdanilin@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.*;

public abstract class UnixToEpoch<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Divide Unix timestamp (Java Long) by 1000 to get Epoch, unix time in seconds (Java Integer).";

    private interface ConfigName {
        String TS_FIELD_NAME = "ts.field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TS_FIELD_NAME, ConfigDef.Type.STRING, "ts", ConfigDef.Importance.HIGH,
                    "Field name of original unix timestamp.");

    private static final String PURPOSE = "Converting Unix timestamp to epoch.";

    private String fieldName;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.TS_FIELD_NAME);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private Object convertUnixToEpoch(Object unix_ts) {
        if (unix_ts == null) {
            return null;
        }
        Long epoch_ts = null;
        if (unix_ts instanceof Number) {
            epoch_ts = (((Number) unix_ts).longValue() / 1000);
        } else if (unix_ts instanceof String) {
            epoch_ts = Long.parseLong((String) unix_ts) / 1000;
        } else {
            throw new DataException("Expected Unix timestamp to be a Long, but found " + unix_ts.getClass());
        }
        return epoch_ts.intValue();
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
        if (value == null) {
            return newRecord(record, null, null);
        }
        Object unix_ts = value.get(fieldName);
        final Map<String, Object> updatedValue = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            if (entry.getKey().equals(fieldName)) {
                updatedValue.put(entry.getKey(), convertUnixToEpoch(unix_ts));
            } else {
                updatedValue.put(entry.getKey(), entry.getValue());
            }
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);

        final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema == null) {
            SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            for (Field field : schema.fields()) {
                builder.field(field.name(), field.schema());
            }
            if (schema.isOptional())
                builder.optional();
            if (schema.defaultValue() != null) {
                Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                builder.defaultValue(updatedDefaultValue);
            }

            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }

        Struct updatedValue = applyValueWithSchema(value, updatedSchema);
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(fieldName)) {
                updatedFieldValue = convertUnixToEpoch(value.get(field));
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private String getRandomUuid() {
        return UUID.randomUUID().toString();
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(fieldName, Schema.STRING_SCHEMA);

        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends UnixToEpoch<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends UnixToEpoch<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}


