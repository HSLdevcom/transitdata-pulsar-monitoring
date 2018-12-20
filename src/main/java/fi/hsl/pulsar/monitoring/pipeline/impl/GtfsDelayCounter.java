package fi.hsl.pulsar.monitoring.pipeline.impl;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.pulsar.monitoring.pipeline.PipelineContext;
import fi.hsl.pulsar.monitoring.pipeline.PipelineResult;
import fi.hsl.pulsar.monitoring.pipeline.PipelineStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class GtfsDelayCounter extends PipelineStep<GtfsRealtime.TripUpdate> {

    private static final Logger log = LoggerFactory.getLogger(GtfsDelayCounter.class);
    final int maxDelayMs;
    final boolean ignoreCancellations;

    public GtfsDelayCounter(Config config) {
        super(config);
        maxDelayMs = config.getInt("pipeline.delayCounter.maxDelayInMs");
        ignoreCancellations = config.getBoolean("pipeline.delayCounter.ignoreCancellations");
        log.info("DelayCounter alerting with maxDelay {}. ignoring cancellations? {}", maxDelayMs, ignoreCancellations);
    }

    @Override
    public void initialize(PipelineContext context) {
        context.setResults(this, new DelayResults(maxDelayMs));
    }

    public static class DelayResults implements PipelineResult {
        final int maxDelayMs;

        public static final long[] binsMs = {10L, 100L, 500L, 1000L, 2500L, 5000L, 10000L, 15000L, 20000L,  30000L, 60000L, Integer.MAX_VALUE };
        long [] histogram = new long[binsMs.length];
        int overMaxCounter = 0;
        long min = Long.MAX_VALUE;
        long max = 0L;

        DelayResults(int maxDelayMs) {
            this.maxDelayMs = maxDelayMs;
        }

        public void addSample(long delayMs) {
            if (delayMs < min) {
                min = delayMs;
            }
            if (delayMs > max) {
                max = delayMs;
            }

            int bin = findBinIndex(delayMs);
            histogram[bin]++;
            if (delayMs > maxDelayMs) {
                overMaxCounter++;
            }
        }

        public static int findBinIndex(long delayMs) {
            for (int index = 0; index < binsMs.length; index++) {
                if (delayMs < binsMs[index]) {
                    return index;
                }
                index++;
            }
            return binsMs.length - 1;
        }

        @Override
        public void clear() {
            histogram = new long[binsMs.length];
            overMaxCounter = 0;
            min = Long.MAX_VALUE;
            max = 0L;
        }

        @Override
        public List<String> results() {
            List<String> results = new LinkedList<>();
            results.add("Delay spread: " + Arrays.toString(histogram) + " (bins: " + Arrays.toString(binsMs) + ")");
            results.add("Min: " + min + "ms, Max: " + max + "ms");
            return results;
        }

        @Override
        public boolean shouldAlert() {
            return overMaxCounter > 0;
        }

        @Override
        public String alertMessage() {
            return overMaxCounter + " messages with larger delay than " + maxDelayMs + "ms where max: " + max + "ms";
        }
    }


    @Override
    public void handleMessage(PipelineContext context, GtfsRealtime.TripUpdate msg) {
        //The cancellation trips are sent continuously in our pipeline so it might make sense to ignore those
        if (ignoreCancellations && msg.getTrip().getScheduleRelationship() == GtfsRealtime.TripDescriptor.ScheduleRelationship.CANCELED)
            return;

        DelayResults results = (DelayResults)context.getResults(this);
        if (results == null) {
            results = new DelayResults(maxDelayMs);
        }
        long now = System.currentTimeMillis();
        long ts = msg.getTimestamp() * 1000;
        long delay = now - ts;
        results.addSample(delay);

        context.setResults(this, results);
    }


}
