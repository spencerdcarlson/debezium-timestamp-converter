package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimestampConverter.class);


    public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String DEFAULT_DATE_FORMAT = "YYYY-MM-dd";
    public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";
    public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp",
            "datetime2");
    private static final Map<String, String> MONTH_MAP = Map.ofEntries(Map.entry("jan", "01"), Map.entry("feb", "02"),
            Map.entry("mar", "03"), Map.entry("apr", "04"), Map.entry("may", "05"), Map.entry("jun", "06"),
            Map.entry("jul", "07"), Map.entry("aug", "08"), Map.entry("sep", "09"), Map.entry("oct", "10"),
            Map.entry("nov", "11"), Map.entry("dec", "12"));
    private static final Pattern dateTimeRegex = Pattern.compile("(?<datetime>(?<date>(?:(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2}))|(?:(?<day2>\\d{1,2})\\/(?<month2>\\d{1,2})\\/(?<year2>\\d{4}))|(?:(?<day3>\\d{1,2})-(?<month3>\\w{3})-(?<year3>\\d{4})))?(?:\\s?T?(?<time>(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})\\.?(?<milli>\\d{0,7})?)?))");
    private static final Pattern numberRegex = Pattern.compile("\\d+");

    private DateTimeFormatter dateTimeFormatter;
    private DateTimeFormatter dateFormatter;
    private DateTimeFormatter timeFormatter;

    private String dateTimePattern, datePattern, timePattern;

    @Override
    public void configure(Properties props) {
        LOGGER.debug("configure properties");
        this.dateTimePattern = props.getProperty("format.datetime", DEFAULT_DATETIME_FORMAT);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(this.dateTimePattern).withZone(ZoneOffset.UTC);

        this.datePattern = props.getProperty("format.date", DEFAULT_DATE_FORMAT);
        this.dateFormatter = DateTimeFormatter.ofPattern(this.datePattern).withZone(ZoneOffset.UTC);

        this.timePattern = props.getProperty("format.time", DEFAULT_TIME_FORMAT);
        this.timeFormatter = DateTimeFormatter.ofPattern(this.timePattern).withZone(ZoneOffset.UTC);

        LOGGER.debug("dateTimePattern: {}, datePattern: {}, timePattern: {}", this.dateTimePattern, this.datePattern, this.timePattern);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        LOGGER.debug("[column] name: {}, type name: {}, has default value: {}, default value: {}, is optional: {}", column.name(), column.typeName(), column.hasDefaultValue(), column.defaultValue(), column.isOptional());
        if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(column.typeName()))) {
            LOGGER.debug("registering column for conversion. typeName: {}", column.typeName());
            // NOTE SUPPORTED_DATA_TYPES decides which types will get converted. We could augment the logic
            // here to just look for a list of column names e.g. "inserted_at", "updated_at", etc

            boolean isTime = "time".equalsIgnoreCase(column.typeName());
            // Use a new SchemaBuilder every time in order to avoid changing "Already set" options
            // in the schema builder between tables.

            // Building the schema for the payload
            final SchemaBuilder builder = column.isOptional() ? SchemaBuilder.string().optional() : SchemaBuilder.string().required();
            registration.register(builder, rawValue -> {
                if (rawValue == null) {
                    LOGGER.debug("value of {} is null", column.name());
                    return fallback(column);
                }
                final String value = rawValue.toString();

                LOGGER.debug("value: {}", value);

                final Instant instant;
                if (isIsoString(value)) {
                    instant = parseIsoString(value);
                } else if (isTime) {
                    instant = parseTime(value);
                } else {
                    instant = parseEpoch(value);
                }
                if (instant == null) {
                    return rawValue.toString();
                }
                LOGGER.debug("instant: {}", instant);
                switch (column.typeName().toLowerCase()) {
                    case "time":
                        LOGGER.debug("using time formatter. {}", this.timePattern);
                        return this.timeFormatter.format(instant);
                    case "date":
                        LOGGER.debug("using date formatter. {}", this.datePattern);
                        return this.dateFormatter.format(instant);
                    default:
                        LOGGER.debug("using datetime formatter. {}", this.dateTimePattern);
                        return this.dateTimeFormatter.format(instant);
                }
            });
        }
    }

    private Object fallback(final RelationalColumn column) {
        if (column.isOptional()) {
            return null;
        } else if (column.hasDefaultValue()) {
            return column.defaultValue();
        }
        return null;
    }

    private Instant parseTime(final String timestamp) {
        long epoch = Long.parseLong(timestamp.replaceAll("\\D+", ""));
        // FIXME: This does not support microseconds.
        return Instant.ofEpochMilli(epoch);
    }

    private Instant parseEpoch(final String datetime) {
        LOGGER.debug("parsing epoch. datetime: {}", datetime);
        if (datetime == null || datetime.isBlank() || !isEpoch(datetime)) {
            return null;
        }
        long epoch = Long.parseLong(datetime);
        LOGGER.debug("parsed epoch. epoch: {}", epoch);
        if (datetime.length() < 6) {
            LOGGER.debug("convert using days");
            return Instant.EPOCH.plus(epoch, ChronoUnit.DAYS);
        } else if (datetime.length() < 14) {
            LOGGER.debug("convert using milliseconds");
            return Instant.EPOCH.plus(epoch, ChronoUnit.MILLIS);
        }
        LOGGER.debug("convert using microseconds");
        return Instant.EPOCH.plus(epoch, ChronoUnit.MICROS);
    }

    private Instant parseIsoString(final String isoString) {
        final String normalized = normalizeIso(isoString);
        if (normalized == null) {
            return null;
        }
        return Instant.parse(normalized);
    }

    private boolean isEpoch(final String timestamp) {
        return numberRegex.matcher(timestamp).matches();
    }

    private boolean isIsoString(final String timestamp) {
        return (timestamp.contains(":") || timestamp.contains("-"));
    }

    private String normalizeIso(final String string) {
        final Matcher matches = dateTimeRegex.matcher(string);

        if (matches.find()) {
            String year = (matches.group("year") != null ? matches.group("year")
                    : (matches.group("year2") != null ? matches.group("year2") : matches.group("year3")));
            String month = (matches.group("month") != null ? matches.group("month")
                    : (matches.group("month2") != null ? matches.group("month2") : matches.group("month3")));
            String day = (matches.group("day") != null ? matches.group("day")
                    : (matches.group("day2") != null ? matches.group("day2") : matches.group("day3")));
            String hour = matches.group("hour") != null ? matches.group("hour") : "00";
            String minute = matches.group("minute") != null ? matches.group("minute") : "00";
            String second = matches.group("second") != null ? matches.group("second") : "00";
            String milli = matches.group("milli") != null ? matches.group("milli") : "000";

            final StringBuilder isoDateString = new StringBuilder();
            isoDateString.append(String.format("%s:%s:%s.%s", ("00".substring(hour.length()) + hour),
                    ("00".substring(minute.length()) + minute), ("00".substring(second.length()) + second),
                    (milli + "000000".substring(milli.length()))));

            if (year != null) {
                if (month.length() > 2)
                    month = MONTH_MAP.get(month.toLowerCase());

                return new StringBuilder(String.format("%s-%s-%sT%sZ", year, ("00".substring(month.length()) + month),
                        ("00".substring(day.length()) + day), isoDateString)).toString();

            } else {
                return new StringBuilder(String.format("%s-%s-%sT%sZ", "2020", "01", "01", isoDateString)).toString();
            }
        }
        return null;
    }
}
