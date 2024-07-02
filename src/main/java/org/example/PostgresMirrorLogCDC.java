package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;


public class PostgresMirrorLogCDC {

    public static void main(String[] args) throws Exception {



        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();
        Tables tableDefinition = new Tables();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("public")
                        .tableList(tableDefinition.getStringTableList())
                        .username("postgres")
                        .password("postgres")
                        .slotName("flink3")
                        .decodingPluginName("decoderbufs") // use pgoutput for PostgreSQL 10+
                        .deserializer(deserializer)
                        .includeSchemaChanges(true) // output the schema changes as well
                        .splitSize(1) // the split size of each snapshot split
                        .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/sv/App/flink-1.18.0/cp");
        env.configure(config);
        CheckpointConfig conf = env.getCheckpointConfig();
            conf.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        env.enableCheckpointing(3000);

        DataStream<String> debeziumMessage =
                env.fromSource(postgresIncrementalSource, WatermarkStrategy.noWatermarks(), "PG").setParallelism(1);

        HashMap<String,DataStream<String>> dataStreamHashMap =new HashMap<>();

        for (String tableName: tableDefinition.tableList) {

            dataStreamHashMap.put(tableName,
            debeziumMessage.filter(
                    (FilterFunction<String>) value -> {
                        JsonReader reader = Json.createReader(new StringReader(value));
                        JsonObject message = reader.readObject();
                        reader.close();
                        String tableNameMess = message.get("source").asJsonObject().getString("schema") + "." + message.get("source").asJsonObject().getString("table");
                        return Objects.equals(tableNameMess, tableName);

                    }
            ));
            List<Tables.Field> fields = tableDefinition.sqlMergeParameters.get(tableName);

            dataStreamHashMap.get(tableName)
                        .addSink(
                                JdbcSink.sink(
                                        tableDefinition.sqlMergeStatement.get(tableName),
                                        (statement, message) -> setMergeParams(statement,message,fields),
                                        JdbcExecutionOptions.builder()
                                                .withBatchSize(100)
                                                .withBatchIntervalMs(200)
                                                .withMaxRetries(1)
                                                .build(),
                                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                .withUrl("jdbc:postgresql://localhost:5433/ods")
                                                .withDriverName("org.postgresql.Driver")
                                                .withUsername("test")
                                                .withPassword("test")
                                                .withConnectionCheckTimeoutSeconds(60)
                                                .build()
                                )
                        );
            List<Tables.Field> fieldsLog = tableDefinition.sqlLogInsertParameters.get(tableName);
            dataStreamHashMap.get(tableName)
                        .addSink(
                                JdbcSink.sink(
                                        tableDefinition.sqlLogInsertStatement.get(tableName),
                                        (statement, message) -> setLogInsertParams(statement,message,fieldsLog),
                                        JdbcExecutionOptions.builder()
                                                .withBatchSize(100)
                                                .withBatchIntervalMs(200)
                                                .withMaxRetries(1)
                                                .build(),
                                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                .withUrl("jdbc:postgresql://localhost:5433/ods")
                                                .withDriverName("org.postgresql.Driver")
                                                .withUsername("test")
                                                .withPassword("test")
                                                .withConnectionCheckTimeoutSeconds(60)
                                                .build()
                                )
                        );
             // use parallelism 1 for sink to keep message ordering
        }


        env.execute("Output Postgres Snapshot");
    }



    private static void setMergeParams(PreparedStatement statement, String messageString, List<Tables.Field> fields) throws SQLException{
        JsonObject message = Json.createReader(new StringReader(messageString)).readObject();
        String row_version = "after";
        String op = message.getString("op");
        Long dbz_ts_ms = message.getJsonNumber("ts_ms").longValue();
        Long src_ts_ms = message.getJsonObject("source").getJsonNumber("ts_ms").longValue();


        if (Objects.equals(op, "d"))
            row_version = "before";

        JsonObject row = message.get(row_version).asJsonObject();
        System.out.println(fields);
        for (Tables.Field field:fields) {
            if (Objects.equals(field.system_field, "false"))
                typeMapping(statement, field, row);
            else{
                if (Objects.equals(field.name, "dbz_op_type"))
                    statement.setString(field.ord,
                            op);
                if (Objects.equals(field.name, "dbz_ts_ms"))
                    statement.setLong(field.ord,
                            dbz_ts_ms);
                if (Objects.equals(field.name, "src_ts_ms"))
                    statement.setLong(field.ord,
                            src_ts_ms);
            }
        }
    }

    private static void typeMapping(PreparedStatement statement, Tables.Field field, JsonObject row) throws SQLException {

            if (!row.isNull(field.name))
                switch (field.type) {
                case "long":
                    statement.setLong(field.ord,
                            row.getJsonNumber(field.name).longValue());
                    break;
                case "string":
                    statement.setString(field.ord,
                            row.getString(field.name));
                    break;
                case "boolean":
                    statement.setBoolean(field.ord,
                            row.getBoolean(field.name));
                    break;
                default:
                    statement.setString(field.ord,
                            row.getString(field.name));
            }
            else
                setParameterNull(statement, field);


    }

    private static void setLogInsertParams(PreparedStatement statement, String messageString, List<Tables.Field> fields) throws SQLException{
        JsonObject message = Json.createReader(new StringReader(messageString)).readObject();
        String row_version = "after";
        String op = message.getString("op");
        Long dbz_ts_ms = message.getJsonNumber("ts_ms").longValue();
        Long src_ts_ms = message.getJsonObject("source").getJsonNumber("ts_ms").longValue();

        System.out.println(message);


        if (!message.isNull("before")) {
            System.out.println("BEFORE NOT NULL");
            JsonObject row_before = message.get("before").asJsonObject();
            for (Tables.Field field:fields){
                if  (Objects.equals(field.stateField, "before"))
                {
                    typeMapping(statement, field, row_before);
                }
            }
        }
        else
        {
            for (Tables.Field field:fields){
                if  (Objects.equals(field.stateField, "before"))
                {
                    setParameterNull(statement, field);
                }
            }
        }


        if (!message.isNull("after")) {
                System.out.println("AFTER NOT NULL");
                JsonObject row_after = message.get("after").asJsonObject();
                for (Tables.Field field : fields) {
                    if (Objects.equals(field.stateField, "after")) {
                        typeMapping(statement, field, row_after);
                    }
                }
            }
        else
        {
            for (Tables.Field field:fields){
                if  (Objects.equals(field.stateField, "after"))
                {
                    setParameterNull(statement, field);
                }
            }
        }

        for (Tables.Field field:fields) {
            if (Objects.equals(field.stateField, "common"))
            {
                if (Objects.equals(field.name, "dbz_op_type"))
                    statement.setString(field.ord,
                            op);
                if (Objects.equals(field.name, "dbz_ts_ms"))
                    statement.setLong(field.ord,
                            dbz_ts_ms);
                if (Objects.equals(field.name, "src_ts_ms"))
                    statement.setLong(field.ord,
                            src_ts_ms);
            }
        }

    }

    private static void setParameterNull(PreparedStatement statement, Tables.Field field) throws SQLException {
        switch (field.type) {
            case "long":
                statement.setNull(field.ord, Types.BIGINT);
                break;
            case "string":
                statement.setNull(field.ord,
                        Types.VARCHAR);
                break;
            case "boolean":
                statement.setNull(field.ord,
                        Types.BOOLEAN);
                break;
            default:
                statement.setNull(field.ord,
                        Types.VARCHAR);
        }
    }
}