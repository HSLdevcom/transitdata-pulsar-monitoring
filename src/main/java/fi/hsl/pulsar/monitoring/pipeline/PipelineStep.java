package fi.hsl.pulsar.monitoring.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

public abstract class PipelineStep<T> {
    protected ObjectMapper mapper = new ObjectMapper();

    Config config;

    public PipelineStep(Config config) {
        this.config = config;
    }

    public abstract void initialize(PipelineContext context);

    public abstract void handleMessage(PipelineContext context, T msg);
}
