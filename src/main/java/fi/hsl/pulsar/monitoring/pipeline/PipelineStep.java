package fi.hsl.pulsar.monitoring.pipeline;

import com.typesafe.config.Config;

public abstract class PipelineStep<T> {
    Config config;

    public PipelineStep(Config config) {
        this.config = config;
    }

    public abstract PipelineContext handleMessage(PipelineContext context, T msg);
}
