package fi.hsl.pulsar.monitoring.pipeline;

import com.google.transit.realtime.GtfsRealtime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripUpdateCounter extends PipelineStep<GtfsRealtime.TripUpdate> {
    private static final Logger log = LoggerFactory.getLogger(TripUpdateCounter.class);

    class CountResults {
        long counter;

        @Override
        public String toString() {
            return "Message count: " + counter;
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
