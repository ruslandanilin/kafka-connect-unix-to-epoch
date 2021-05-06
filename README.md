Kafka Connect SMT to convert Unix timestamp (with milliseconds) to Unix Epoch (in seconds). Or just divide Java Long value by 1000 to get an Integer value.
This SMT supports converting both Key or Value.

Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`ts.field.name`| Field name of Unix timestamp | String | `ts` | High |

Example on how to add to your connector:
```
transforms=unixtoepoch
transforms.unixtoepoch.type=org.kafka.connect.smt.UnixToEpoch$Value
transforms.unixtoepoch.ts.field.name="ts"
```

To build JAR file use:
```
mvn clean package
```

Skeleton got from https://github.com/confluentinc/kafka-connect-insert-uuid
And from the Apache Kafka® `InsertField` and `TimestampConvert` SMT.
