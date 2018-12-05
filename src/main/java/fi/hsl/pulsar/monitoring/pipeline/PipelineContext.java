package fi.hsl.pulsar.monitoring.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pulsar.shade.com.google.gson.JsonObject;
import org.apache.pulsar.shade.org.apache.avro.data.Json;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PipelineContext {
    private final ObjectMapper mapper = new ObjectMapper();

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
        ObjectNode root = mapper.createObjectNode();
        resultsPerStep.forEach((key, value) -> {
            root.set(key.getClass().getSimpleName(), value.results());
        });
        /*List<JsonNode> nodes = resultsPerStep.entrySet().stream().flatMap(
                entry -> entry.getValue().results().stream()).collect(Collectors.toList());
        ArrayNode root = mapper.createArrayNode();*/

        //node.set()
        //root.addAll(nodes);
        try {
            return mapper.writeValueAsString(root);
        }
        catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
        /*return resultsPerStep.entrySet().stream().flatMap(
                entry -> entry.getValue().results().stream()
                        .map(resultRow ->
                            entry.getKey().getClass().getSimpleName() + " : " + resultRow)
        ).collect(Collectors.toList());*/

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

    public synchronized String getResultsAndClear() {
        String results = resultsAsString();
        clearResults();
        return results;
    }
}
