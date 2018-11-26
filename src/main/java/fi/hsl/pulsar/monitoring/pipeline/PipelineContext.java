package fi.hsl.pulsar.monitoring.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipelineContext {

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

    public synchronized List<String> resultsAsString() {
        return resultsPerStep.entrySet().stream().flatMap(
                entry -> entry.getValue().results().stream()
                        .map(resultRow ->
                            entry.getKey().getClass().getSimpleName() + " : " + resultRow)
        ).collect(Collectors.toList());
    }

    public synchronized List<String> getAlerts() {
        return resultsPerStep.entrySet().stream().flatMap(
                entry -> {
                    PipelineResult result = entry.getValue();
                    if (result.shouldAlert()) {
                        return Stream.of(entry.getKey().getClass().getSimpleName() + " : " + result.alertMessage());
                    } else {
                        return Stream.empty();
                    }
                }
        ).collect(Collectors.toList());
    }

    public synchronized List<String> getResultsAndClear() {
        List<String> results = resultsAsString();
        clearResults();
        return results;
    }
}
