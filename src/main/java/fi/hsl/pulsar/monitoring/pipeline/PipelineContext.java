package fi.hsl.pulsar.monitoring.pipeline;

import java.util.HashMap;

public class PipelineContext {

    HashMap<PipelineStep, Object> resultsPerStep = new HashMap<>();

    public synchronized HashMap<PipelineStep, Object> getResults() {
        return resultsPerStep;
    }

    public synchronized void clearResults() {
        resultsPerStep.clear();
    }

    public synchronized void setResults(PipelineStep owner,  Object results) {
        resultsPerStep.put(owner, results);
    }

    public synchronized Object getResults(PipelineStep owner) {
        return resultsPerStep.get(owner);
    }

    public synchronized String resultsAsString() {
        StringBuilder builder = new StringBuilder();
        resultsPerStep.entrySet().forEach(
                entry -> builder.append(entry.getKey().getClass().getSimpleName())
                .append(" : ")
                .append(entry.getValue().toString())
                .append("\n")
        );

        return builder.toString();
    }
}
