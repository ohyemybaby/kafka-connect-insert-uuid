package com.github.cjmatta.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * @program: TimestampTimeZoneConverter
 * @description: debezium会默认把timestamp格式的字段修改为UTC时区，这个可能用着不是很方便，我们自定义修改一下
 * @author: Mark.Sun
 * @create: 2023-02-24 13:52
 **/
public abstract class TimestampTimeZoneConverter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TimestampTimeZoneConverter.class);


    public static final String overivew_doc =
            "Update the record's topic field as a function of the original topic value and the record timestamp."
                    + "<p/>"
                    + "This is mainly useful for sink connectors, since the topic field is often used to determine the equivalent entity name in the destination system"
                    + "(e.g. database table or search index name).";
    private static final String DEFAULT_UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
    private static final String DEFAULT_BAK_UTC_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String DEFAULT_LOCAL_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final String DEFAULT_TIME_ZONE = "GMT+08:00";
    private static final String DEFAULT_UTC = "GMT";
    private static final String DEFAULT_TIMESTAMP_FIELD = "UPDATE_TIME";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.MESSAGE_TIMESTAMP_FORMAT, ConfigDef.Type.STRING, DEFAULT_LOCAL_FORMAT, ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.BAK_DEBEZIUM_TIMESTAMP_FORMAT, ConfigDef.Type.STRING, DEFAULT_BAK_UTC_FORMAT, ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.TIMEZONE, ConfigDef.Type.STRING, DEFAULT_TIME_ZONE, ConfigDef.Importance.HIGH,
                    "TIMEZONE")
            .define(ConfigName.MESSAGE_TIMESTAMP_FIELD, ConfigDef.Type.STRING, DEFAULT_TIMESTAMP_FIELD, ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.");

    private interface ConfigName {
        String MESSAGE_TIMESTAMP_FORMAT = "message.timestamp.format";
        String BAK_DEBEZIUM_TIMESTAMP_FORMAT = "bak.debezium.timestamp.format";
        String TIMEZONE = "message.timestamp.timezone";
        String MESSAGE_TIMESTAMP_FIELD = "message.timestamp.fields";
    }

    private String fieldName;

    private ThreadLocal<DateTimeFormatter> localFormatter, bakUtcFormatter;

    private static final String PURPOSE = "时间戳字段转换时区";

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);

        this.bakUtcFormatter = ThreadLocal.withInitial(() ->
                DateTimeFormatter
                        .ofPattern(config.getString(ConfigName.BAK_DEBEZIUM_TIMESTAMP_FORMAT))
                        .withZone(ZoneId.of(DEFAULT_UTC)));

        this.localFormatter = ThreadLocal.withInitial(() ->
                DateTimeFormatter
                        .ofPattern(config.getString(ConfigName.MESSAGE_TIMESTAMP_FORMAT))
                        .withZone(ZoneId.of(config.getString(ConfigName.TIMEZONE))));
        fieldName = config.getString(ConfigName.MESSAGE_TIMESTAMP_FIELD);
        log.info("-------------------------configure begin-----------------------------------");
        log.info("----fieldName: " + fieldName);
        log.info("----TIMEZONE: " + config.getString(ConfigName.TIMEZONE));
        log.info("----MESSAGE_TIMESTAMP_FORMAT: " + config.getString(ConfigName.MESSAGE_TIMESTAMP_FORMAT));
        log.info("----BAK_DEBEZIUM_TIMESTAMP_FORMAT: " + config.getString(ConfigName.BAK_DEBEZIUM_TIMESTAMP_FORMAT));
        log.info("-------------------------configure end-----------------------------------");
    }

    @Override
    public R apply(R record) {


        if(record.value() == null) {
            return null;
        } else if (operatingSchema(record) == null) {
            log.info("=========================record" + record);
            log.info("=========================operatingSchema" + operatingSchema(record));
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    protected Object converter(Object value) {
        String localTimestampStr = "";
        if (value == null) {
            return value;
        } else {
            try {
                ZonedDateTime zdt = ZonedDateTime.parse(value.toString());
                localTimestampStr = localFormatter.get().format(zdt);
            } catch (DateTimeParseException e) {
                log.error("DateTimeParseException :" + e + " value: " + value, e);
                LocalDateTime ldt = LocalDateTime.parse(value.toString(), bakUtcFormatter.get());
                localTimestampStr = localFormatter.get().format(ldt);
            }
            return localTimestampStr;
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        String[] fields = fieldName.split(",");
        if (fields != null && fields.length > 0){
            for (String field : fields) {
                try {
                    Object o = updatedValue.get(field);
                    if (o == null){
                        return record;
                    }
                    updatedValue.put(field, converter(o));
                } catch (DateTimeParseException e) {
                    log.error("bakException :" + e + " value: " + value + " object:" + record, e);
                } catch (Exception ex){
                    log.error("record: " + record);
                    log.error("Exception: ", ex);
                }
            }

        }

        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        if (record.topic().contains("debezium")) {
            return record;
        }
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        final Struct updatedValue = new Struct(value.schema());

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        String[] fields = fieldName.split(",");
        if (fields != null && fields.length > 0) {
            for (String field : fields) {
                try {
                    updatedValue.put(field, converter(updatedValue.get(field)));
                } catch (DataException de){
                    log.error("record: " + record);
                    log.error("DataException: ", de);
                }catch (DateTimeParseException e) {
                    log.error("DateTimeParseException :" + e + " value: " + value + " object:" + record,e);
                } catch (Exception ex){
                    log.error("record: " + record);
                    log.error("Exception: ", ex);
                }
            }

        }
        return newRecord(record, updatedValue);
    }




    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }


    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Object updateValue);




    public static class Key<R extends ConnectRecord<R>> extends TimestampTimeZoneConverter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends TimestampTimeZoneConverter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }

    }
}