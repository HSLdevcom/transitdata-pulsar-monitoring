package fi.hsl.pulsar.monitoring.pipeline;

import com.google.transit.realtime.GtfsRealtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripUpdateCounter extends PipelineStep<GtfsRealtime.TripUpdate> {
    private static final Logger log = LoggerFactory.getLogger(TripUpdateCounter.class);

    public TripUpdateCounter() {
        super();
    }

    public TripUpdateCounter(PipelineStep parent) {
        super(parent);
    }


    class CountResults implements PipelineContext.PipelineResult {
        long counter;
        long startTime = System.currentTimeMillis();

        @Override
        public void clear() {
            counter = 0;
            startTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            long elapsed = System.currentTimeMillis() - startTime;
            Float ratePerSec = elapsed > 0 ? 1000 * (float)counter / (float)elapsed : Float.NaN;

            return "Message rate msg/sec: " + ratePerSec + " (total: " + counter + ")";
        }
    }

    @Override
    PipelineContext handleInternal(PipelineContext context, GtfsRealtime.TripUpdate msg) {
        CountResults results = (CountResults)context.getResults(this);
        if (results == null) {
            results = new CountResults();
        }
        results.counter++;

        context.setResults(this, results);
        return context;
    }
}
