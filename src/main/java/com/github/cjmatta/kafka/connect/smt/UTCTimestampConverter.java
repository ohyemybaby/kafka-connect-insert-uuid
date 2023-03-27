package com.github.cjmatta.kafka.connect.smt;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.TimeZone;

public class UTCTimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {


    private static final String UTC_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
    private static final String UTC_FORMAT_NO_POINT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String LOCAL_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private static final String TIME_ZONE = "GMT+08:00";

    private SimpleDateFormat utcFormatter, localFormatter, utcNoPointFormatter, localNoPointFormatter;
    private DateTimeFormatter dateTimeFormatter;
    private String timezone;

    @Override
    public void configure(Properties props) {
        String utc_format = props.getProperty("utc_format", UTC_FORMAT);
        String utc_format_no_point = props.getProperty("utc_format_no_point", UTC_FORMAT_NO_POINT);
        String local_format = props.getProperty("local_format", LOCAL_FORMAT);
        this.timezone = props.getProperty("timezone", TIME_ZONE);

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(LOCAL_FORMAT);
        this.utcFormatter = new SimpleDateFormat(utc_format);
        this.utcNoPointFormatter = new SimpleDateFormat(utc_format_no_point);
        this.localFormatter = new SimpleDateFormat(local_format);
        this.localNoPointFormatter = new SimpleDateFormat(local_format);

        this.utcFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.utcNoPointFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        this.localFormatter.setTimeZone(TimeZone.getTimeZone(timezone));
        this.localNoPointFormatter.setTimeZone(TimeZone.getTimeZone(timezone));

    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {

        if ("TIMESTAMP".equalsIgnoreCase(column.typeName())) {
            registration.register(SchemaBuilder.string().optional(), value -> {

                String localTimestampStr = "";

                if (value == null){
                    if (column.isOptional()){
                        return null;
                    }
                    else if (column.hasDefaultValue()) {
                        return column.defaultValue();
                    }
                    else
                    {
                        return localTimestampStr;
                    }
                }
                String valueText = value.toString();
                try {
                    ZonedDateTime zdt = ZonedDateTime.parse(valueText);
                    zdt = zdt.withZoneSameInstant(ZoneId.of(timezone));
                    LocalDateTime localDateTime = zdt.toLocalDateTime();
                    localTimestampStr = dateTimeFormatter.format(localDateTime);
                } catch (RuntimeException e) {
                    System.out.println("Exception :" + e + "　value: " + valueText);
                    localTimestampStr = value.toString();
                }

                return localTimestampStr;
            });
        }
    }
}
