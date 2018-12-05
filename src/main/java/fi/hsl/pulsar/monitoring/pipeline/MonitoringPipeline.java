package fi.hsl.pulsar.monitoring.pipeline;

import com.google.transit.realtime.GtfsRealtime;
import com.typesafe.config.Config;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.transitdata.TransitdataProperties;
import fi.hsl.pulsar.monitoring.pipeline.impl.GtfsDelayCounter;
import fi.hsl.pulsar.monitoring.pipeline.impl.MessageCounter;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MonitoringPipeline implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MonitoringPipeline.class);

    /**
     * Later we possibly want to monitor different types of messages than TripUpdates.
     * Then either remove the type from PipelineStep (con: reduced compile time checks)
     * or store multiple different pipelines.
     *
     * Perhaps best solution could be to turn this class into abstract one with type parameter T
     * and then have different implementations per type
     */
    private final List<PipelineStep<GtfsRealtime.TripUpdate>> pipelineSteps;

    private final PipelineContext context = new PipelineContext();
    final ScheduledExecutorService scheduler;

    private MonitoringPipeline(Config config) {
        long resultIntervalInSecs = config.getInt("pipeline.resultIntervalInSecs");
        log.info("Result interval {} seconds", resultIntervalInSecs);
        pipelineSteps = new LinkedList<>();
        if (config.getBoolean("pipeline.messageCounter.enabled")) {
            log.info("Adding MessageCounter");
            pipelineSteps.add(new MessageCounter<GtfsRealtime.TripUpdate>(config));
        }
        if (config.getBoolean("pipeline.delayCounter.enabled")) {
            log.info("Adding DelayCounter");
            pipelineSteps.add(new GtfsDelayCounter(config));
        }

        pipelineSteps.forEach(step -> step.initialize(context));

        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting result-scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                log.debug("Checking results!");
                context.getAlerts().forEach(alert -> log.error("ALERT: " + alert));
                //List<String> results = context.getResultsAndClear();
                //results.forEach(r -> log.info(r));
                log.info(context.getResultsAndClear());
            }
            catch (Exception e) {
                log.error("Failed to check results", e);
            }

        }, resultIntervalInSecs, resultIntervalInSecs, TimeUnit.SECONDS);
    }

    public static MonitoringPipeline newPipeline(Config config) {
        //We could initialize different pipelines based on configs / arguments
        return new MonitoringPipeline(config);
    }

    public void handleMessage(final Message msg) throws Exception {
        parseProtobufSchema(msg).ifPresent(schema -> {
            try {
                if (schema == TransitdataProperties.ProtobufSchema.GTFS_TripUpdate) {
                    handleTripUpdateMessage(msg);
                }
                else {
                    log.info("Ignoring message of schema " + schema);
                }
            }
            catch (Exception e) {
                log.error("Failed to handle message for schema " + schema, e);
            }
        });
    }

    private void handleTripUpdateMessage(final Message msg) throws Exception {
        GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());

        List<GtfsRealtime.TripUpdate> tripUpdates = feedMessage.getEntityList()
                .stream()
                .flatMap(
                        entity -> entity.hasTripUpdate() ? Stream.of(entity.getTripUpdate()) : Stream.empty()
                ).collect(Collectors.toList());

        for (GtfsRealtime.TripUpdate tu : tripUpdates) {
            for (PipelineStep<GtfsRealtime.TripUpdate> step : pipelineSteps) {
                step.handleMessage(context, tu);
            }
        }
    }


    private Optional<TransitdataProperties.ProtobufSchema> parseProtobufSchema(Message received) {
        try {
            String schemaType = received.getProperty(TransitdataProperties.KEY_PROTOBUF_SCHEMA);
            log.debug("Received message with schema type " + schemaType);
            TransitdataProperties.ProtobufSchema schema = TransitdataProperties.ProtobufSchema.fromString(schemaType);
            return Optional.of(schema);
        }
        catch (Exception e) {
            //log.error("Failed to parse protobuf schema", e);
            //return Optional.empty();
            //DEBUG, now the TripUpdateProcessor doesn't yet output this. TODO fix
            return Optional.of(TransitdataProperties.ProtobufSchema.GTFS_TripUpdate);
        }
    }

    public void close() {
        log.info("Closing pipeline");
        scheduler.shutdown();
    }
}
