package com.github.cjmatta.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;



/**
 * @program: InsertUuid
 * @description:
 * @author: Mark.Sun
 * @create: 2023-02-27 09:31
 **/

public abstract class MessageFieldRouter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageFieldRouter.class);

    private static final Pattern TOPIC = Pattern.compile("${topic}", Pattern.LITERAL);

    private static final Pattern Field = Pattern.compile("${field}", Pattern.LITERAL);

    public static final String overivew_doc =
            "Update the record's topic field as a function of the original topic value and the record timestamp."
                    + "<p/>"
                    + "This is mainly useful for sink connectors, since the topic field is often used to determine the equivalent entity name in the destination system"
                    + "(e.g. database table or search index name).";
    private static final String UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FORMAT, ConfigDef.Type.STRING, "${topic}-${field}", ConfigDef.Importance.HIGH,
                    "Format string which can contain <code>${topic}</code> and <code>${timestamp}</code> as placeholders for the topic and timestamp, respectively.")
            .define(ConfigName.TOPIC_FIELD_LENGTH, ConfigDef.Type.INT, 6, ConfigDef.Importance.HIGH,
                    "java‘s <code>java.lang.String.substring</code>,as you know from 0 beginning;  positive .")
            .define(ConfigName.MESSAGE_TIMESTAMP_FIELD, ConfigDef.Type.STRING, "month", ConfigDef.Importance.HIGH,
                    "java‘s <code>java.lang.String.substring</code>,as you know from 0 beginning;  positive .");

    private interface ConfigName {
        String TOPIC_FORMAT = "topic.format";
        String TOPIC_FIELD_LENGTH = "message.field.length";
        String MESSAGE_TIMESTAMP_FIELD = "message.timestamp.fields";
    }

    private String topicFormat;
    private int fieldLength;
    private String fieldName;


    /**
     * "transforms": "MessageTimestampRouter",
     * "transforms.MessageTimestampRouter.type": "io.confluent.connect.transforms.MessageTimestampRouter",
     * "transforms.MessageTimestampRouter.topic.format": "foo-${topic}-${timestamp}",
     * "transforms.MessageTimestampRouter.message.timestamp.format": "yyyy-MM-dd",
     * "transforms.MessageTimestampRouter.topic.timestamp.format": "yyyy.MM.dd",
     * "transforms.MessageTimestampRouter.message.timestamp.fields": "create_time"
     * 获取配置的函数，可以把配置放置到成员变量上
     */
    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        topicFormat = config.getString(ConfigName.TOPIC_FORMAT);
        fieldLength = config.getInt(ConfigName.TOPIC_FIELD_LENGTH);
        fieldName = config.getString(ConfigName.MESSAGE_TIMESTAMP_FIELD);
    }

    private static final String PURPOSE = "抽取业务时间戳";

    @Override
    public R apply(R record) {
        Object fieldValue = new Object();
        final Schema schema = operatingSchema(record);
        try {
            if (schema == null) {
                final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
                fieldValue = value == null ? null : value.get(fieldName);
            } else {
                final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
                fieldValue = value == null ? null : value.get(fieldName);
            }
        }catch (DataException dataException){
            log.error("该字段可能不存在，数据格式可能出问题", dataException);
        }


        String fieldValueStr = null;
        if (fieldValue  != null && fieldValue instanceof Double ){
            Double aDouble = (Double) fieldValue;
            fieldValueStr = String.valueOf(aDouble);
        } else if(fieldValue instanceof BigInteger){
            BigInteger fieldValue1 = (BigInteger) fieldValue;
            fieldValueStr = String.valueOf(fieldValue1);
        } else if(fieldValue instanceof Character){
            fieldValueStr = fieldValue.toString();
        } else if(fieldValue instanceof String){
            fieldValueStr = fieldValue.toString();
        } else if (fieldValue instanceof Long){
            Long along = (Long)fieldValue;
            fieldValueStr = String.valueOf(along);
        }
        else {
            fieldValueStr = "";
        }

        if (fieldValueStr.length() > fieldLength){
            fieldValueStr = fieldValueStr.substring(0, fieldLength);
        }

        final String replace1 = TOPIC.matcher(topicFormat).replaceAll(Matcher.quoteReplacement(record.topic()));
        final String updatedTopic = Field.matcher(replace1).replaceAll(Matcher.quoteReplacement(fieldValueStr));

        //生成新的记录
        return newRecord(record, updatedTopic);
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

    protected abstract R newRecord(R record, String topic);


    public static class Key<R extends ConnectRecord<R>> extends MessageFieldRouter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, String topic) {
            return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends MessageFieldRouter<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, String topic) {
            return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
        }

    }
}