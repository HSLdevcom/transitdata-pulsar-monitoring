package fi.hsl.pulsar.monitoring.pipeline;

import java.util.HashMap;

public class PipelineContext {

    public interface PipelineResult {
        void clear();
        @Override
        String toString();
    }

    HashMap<PipelineStep, PipelineResult> resultsPerStep = new HashMap<>();

    public synchronized HashMap<PipelineStep, PipelineResult> getResults() {
        return resultsPerStep;
    }

    public synchronized void clearResults() {
        resultsPerStep.values().forEach(PipelineResult::clear);
    }

    public synchronized void setResults(PipelineStep owner,  PipelineResult results) {
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

    public synchronized String getResultsAndClear() {
        String results = resultsAsString();
        clearResults();
        return results;
    }
}
