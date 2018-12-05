package fi.hsl.pulsar.monitoring.pipeline;

import com.fasterxml.jackson.databind.JsonNode;

public interface PipelineResult {
    void clear();

    JsonNode results();

    boolean shouldAlert();

    String alertMessage();
}
