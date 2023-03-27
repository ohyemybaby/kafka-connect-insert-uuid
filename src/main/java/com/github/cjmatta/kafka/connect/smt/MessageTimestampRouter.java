package com.github.cjmatta.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

/**
 * @program: UTCTimestampConverter
 * @description:
 * @author: Mark.Sun
 * @create: 2023-02-24 13:52
 **/
public abstract class MessageTimestampRouter<R extends ConnectRecord<R>> implements Transformation<R>, AutoCloseable {

    private static final Pattern TOPIC = Pattern.compile("${topic}", Pattern.LITERAL);

    private static final Pattern TIMESTAMP = Pattern.compile("${timestamp}", Pattern.LITERAL);

    public static final String overivew_doc =
            "Update the record's topic field as a function of the original topic value and the record timestamp."
                    + "<p/>"
                    + "This is mainly useful for sink connectors, since the topic field is often used to determine the equivalent entity name in the destination system"
                    + "(e.g. database table or search index name).";
    private static final String UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TOPIC_FORMAT, ConfigDef.Type.STRING, "${topic}-${timestamp}", ConfigDef.Importance.HIGH,
                    "Format string which can contain <code>${topic}</code> and <code>${timestamp}</code> as placeholders for the topic and timestamp, respectively.")
            .define(ConfigName.MESSAGE_TIMESTAMP_FORMAT, ConfigDef.Type.STRING, UTC_FORMAT, ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.TOPIC_TIMESTAMP_FORMAT, ConfigDef.Type.STRING, "yyyyMM", ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.MESSAGE_TIMESTAMP_TYPE, ConfigDef.Type.STRING, "snapp.kafka.connect.util.MessageTimestampRouter$Value", ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.")
            .define(ConfigName.MESSAGE_TIMESTAMP_FIELD, ConfigDef.Type.STRING, "create_time", ConfigDef.Importance.HIGH,
                    "Format string for the timestamp that is compatible with <code>java.text.SimpleDateFormat</code>.");

    private interface ConfigName {
        String TOPIC_FORMAT = "topic.format";
        String MESSAGE_TIMESTAMP_FORMAT = "message.timestamp.format";
        String TOPIC_TIMESTAMP_FORMAT = "topic.timestamp.format";
        String MESSAGE_TIMESTAMP_TYPE = "message.timestamp.type";
        String MESSAGE_TIMESTAMP_FIELD = "message.timestamp.fields";
    }

    private String topicFormat;
    private String fieldName;
    private ThreadLocal<SimpleDateFormat> topicTimestampFormat, messageTimestampFormat;


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
        fieldName = config.getString(ConfigName.MESSAGE_TIMESTAMP_FIELD);

        topicTimestampFormat = ThreadLocal.withInitial(() -> {
            final SimpleDateFormat fmt = new SimpleDateFormat(config.getString(ConfigName.TOPIC_TIMESTAMP_FORMAT));
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            return fmt;
        });

        messageTimestampFormat = ThreadLocal.withInitial(() -> {
            final SimpleDateFormat fmt = new SimpleDateFormat(config.getString(ConfigName.MESSAGE_TIMESTAMP_FORMAT));
            fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
            return fmt;
        });
    }

    private static final String PURPOSE = "抽取业务时间戳";

    @Override
    public R apply(R record) {
        Object fieldValue = new Object();
        final Schema schema = operatingSchema(record);
            if (schema == null) {
                final Map<String, Object> value = requireMapOrNull(operatingValue(record), PURPOSE);
                fieldValue = value == null ? null : value.get(fieldName);
            } else {
                final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
                fieldValue = value == null ? null : value.get(fieldName);
            }
        String timestamp = (String)fieldValue;

        //拼装topic
        String formattedTimestamp = "";
        try {
            Date parse = messageTimestampFormat.get().parse(timestamp);
            formattedTimestamp = topicTimestampFormat.get().format(parse);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        final String replace1 = TOPIC.matcher(topicFormat).replaceAll(Matcher.quoteReplacement(record.topic()));
        final String updatedTopic = TIMESTAMP.matcher(replace1).replaceAll(Matcher.quoteReplacement(formattedTimestamp));

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




    public static class Key<R extends ConnectRecord<R>> extends MessageTimestampRouter<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends MessageTimestampRouter<R> {

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