package fi.hsl.pulsar.monitoring.pipeline;

import com.typesafe.config.Config;

public abstract class PipelineStep<T> {
    Config config;

    public PipelineStep(Config config) {
        this.config = config;
    }

    public abstract void initialize(PipelineContext context);

    public abstract void handleMessage(PipelineContext context, T msg);
}
