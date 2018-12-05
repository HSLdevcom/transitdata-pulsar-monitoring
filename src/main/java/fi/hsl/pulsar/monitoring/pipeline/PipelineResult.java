package fi.hsl.pulsar.monitoring.pipeline;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public interface PipelineResult {
    void clear();

    List<JsonNode> results();

    boolean shouldAlert();

    String alertMessage();
}
