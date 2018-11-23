package fi.hsl.pulsar.monitoring.pipeline.impl;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.pulsar.monitoring.pipeline.PipelineContext;
import fi.hsl.pulsar.monitoring.pipeline.PipelineResult;
import fi.hsl.pulsar.monitoring.pipeline.PipelineStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class GtfsRouteCounter extends PipelineStep<GtfsRealtime.TripUpdate> {
    private static final Logger log = LoggerFactory.getLogger(GtfsRouteCounter.class);

    class RouteCountResults implements PipelineResult {

        private static final int PRINT_TOP_COUNT = 5;

        HashMap<String, Long> routes = new HashMap<>();

        public void incRoute(String route) {
            long count = routes.getOrDefault(route, 0L);
            routes.put(route, count + 1);
        }

        @Override
        public void clear() {
            routes.clear();
        }

        @Override
        public List<String> results() {
            List<String> results = new LinkedList<>();
            try {
                int numberOfRoutes = routes.keySet().size();

                List<Map.Entry<String, Long>> entries = routes.entrySet()
                        .stream()
                        .sorted((entry1, entry2) -> Long.compare(entry2.getValue(), entry1.getValue()))
                        .limit(PRINT_TOP_COUNT)
                        .collect(Collectors.toList());

                results.add("No of different routes in total: " + numberOfRoutes);
                results.add("Top " + PRINT_TOP_COUNT + " routes are:");
                results.addAll(entries.stream().map(entry ->
                        entry.getKey() + " : " + entry.getValue())
                    .collect(Collectors.toList()));
            }
            catch (Exception e) {
                log.error("Failed to build RouteCountResults", e);
            }
            return results;
        }
    }

    @Override
    protected PipelineContext handleInternal(PipelineContext context, GtfsRealtime.TripUpdate msg) {
        RouteCountResults results = (RouteCountResults)context.getResults(this);
        if (results == null) {
            results = new RouteCountResults();
        }
        String routeId = msg.getTrip().getRouteId();
        results.incRoute(routeId);

        context.setResults(this, results);
        return context;
    }
}
