package fi.hsl.pulsar.monitoring.pipeline;

import java.util.List;

public interface PipelineResult {
    void clear();

    List<String> results();

    boolean shouldAlert();

    String alertMessage();
}
