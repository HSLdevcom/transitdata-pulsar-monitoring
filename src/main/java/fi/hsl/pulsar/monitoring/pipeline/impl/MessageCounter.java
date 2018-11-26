package fi.hsl.pulsar.monitoring.pipeline.impl;

import com.typesafe.config.Config;
import fi.hsl.pulsar.monitoring.pipeline.PipelineContext;
import fi.hsl.pulsar.monitoring.pipeline.PipelineResult;
import fi.hsl.pulsar.monitoring.pipeline.PipelineStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageCounter extends PipelineStep<Object> {
    private static final Logger log = LoggerFactory.getLogger(MessageCounter.class);

    final int alertThreshold;
    final LocalTime activeStart;
    final LocalTime activeEnd;
    final boolean alertActive;
    final ZoneId zoneId;

    public MessageCounter(Config config) {
        this(config, null);
    }

    public MessageCounter(Config config, PipelineStep parent) {
        super(config, parent);

        alertActive = config.getBoolean("pipeline.messageCounter.alertActive");
        alertThreshold = config.getInt("pipeline.messageCounter.alertIfMessagesBelow");
        activeStart = LocalTime.parse(config.getString("pipeline.messageCounter.alertActiveStart"));
        activeEnd = LocalTime.parse(config.getString("pipeline.messageCounter.alertActiveEnd"));
        zoneId = ZoneId.of(config.getString("pipeline.messageCounter.timezone"));

        if (alertActive) {
            log.info("MessageCounter alert is enabled, active between {} and {} in zone {}", activeStart, activeEnd, zoneId);
        } else {
            log.info("MessageCounter alert disabled");
        }
    }

    static boolean isBetween(LocalTime now, LocalTime start, LocalTime end) {
        if (start.isBefore(end)) {
            return now.isAfter(start) && now.isBefore(end);
        }
        else {
            //ending is > midnight but < start
            return now.isBefore(end) || now.isAfter(start);
        }
    }

    boolean isActiveTime() {
        LocalTime now = LocalTime.now(zoneId);
        return isBetween(now, activeStart, activeEnd);
    }

    class CountResults implements PipelineResult {
        long counter;
        long startTime = System.currentTimeMillis();

        @Override
        public void clear() {
            counter = 0;
            startTime = System.currentTimeMillis();
        }

        @Override
        public List<String> results() {
            long elapsed = System.currentTimeMillis() - startTime;
            Float ratePerSec = elapsed > 0 ? 1000 * (float)counter / (float)elapsed : Float.NaN;

            return Arrays.asList("Message rate msg/sec: " + ratePerSec + " (total: " + counter + ")");
        }

        @Override
        public boolean shouldAlert() {
            return alertActive && isActiveTime() && counter < alertThreshold;
        }

        @Override
        public String alertMessage() {
            return "Alert, message count is below " + alertThreshold + " (was " + counter + ")";
        }
    }

    @Override
    protected PipelineContext handleInternal(PipelineContext context, Object msg) {
        CountResults results = (CountResults)context.getResults(this);
        if (results == null) {
            results = new CountResults();
        }
        results.counter++;

        context.setResults(this, results);
        return context;
    }
}
