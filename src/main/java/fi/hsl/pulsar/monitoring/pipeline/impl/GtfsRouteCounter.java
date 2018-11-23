package fi.hsl.pulsar.monitoring.pipeline.impl;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.pulsar.monitoring.pipeline.PipelineContext;
import fi.hsl.pulsar.monitoring.pipeline.PipelineStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class GtfsRouteCounter extends PipelineStep<GtfsRealtime.TripUpdate> {
    private static final Logger log = LoggerFactory.getLogger(GtfsRouteCounter.class);

    class RouteCountResults implements PipelineContext.PipelineResult {

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
        public String toString() {
            try {

                int numberOfRoutes = routes.keySet().size();

                List<Map.Entry<String, Long>> entries = routes.entrySet()
                        .stream()
                        .sorted((entry1, entry2) -> Long.compare(entry2.getValue(), entry1.getValue()))
                        .limit(PRINT_TOP_COUNT)
                        .collect(Collectors.toList());

                StringBuilder builder = new StringBuilder();
                builder.append("No of routes total: ").append(numberOfRoutes).append("\n");
                builder.append("Top ").append(PRINT_TOP_COUNT).append(" routes are: \n");
                entries.forEach(entry -> builder.append(entry.getKey()).append(" : ").append(entry.getValue()).append("\n"));
                return builder.toString();
            }
            catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }

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
