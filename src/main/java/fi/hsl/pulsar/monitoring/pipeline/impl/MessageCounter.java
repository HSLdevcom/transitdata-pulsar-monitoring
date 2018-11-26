package fi.hsl.pulsar.monitoring.pipeline.impl;

import com.typesafe.config.Config;
import fi.hsl.pulsar.monitoring.pipeline.PipelineContext;
import fi.hsl.pulsar.monitoring.pipeline.PipelineResult;
import fi.hsl.pulsar.monitoring.pipeline.PipelineStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageCounter extends PipelineStep<Object> {
    private static final Logger log = LoggerFactory.getLogger(MessageCounter.class);

    public MessageCounter(Config config) {
        super(config);
    }

    public MessageCounter(Config config, PipelineStep parent) {
        super(config, parent);
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
