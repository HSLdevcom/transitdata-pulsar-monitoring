package fi.hsl.pulsar.monitoring.pipeline;

import com.google.transit.realtime.GtfsRealtime;
import com.sun.xml.internal.bind.v2.util.CollisionCheckStack;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MonitoringPipeline implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MonitoringPipeline.class);

    private TripUpdateCounter tripUpdatePipeline;
    private PipelineContext context = new PipelineContext();
    final ScheduledExecutorService scheduler;

    private MonitoringPipeline(int pollIntervalSecs) {
        tripUpdatePipeline = new TripUpdateCounter(new RouteCounter());

        scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            String results = context.getResultsAndClear();
            log.info(results);
        }, pollIntervalSecs, pollIntervalSecs, TimeUnit.SECONDS);
    }

    public static MonitoringPipeline newPipeline() {
        //We could initialize different pipelines based on configs / arguments
        return new MonitoringPipeline(10);
    }

    public void handleMessage(final Message msg) throws Exception {
        parseProtobufSchema(msg).ifPresent(schema -> {
            try {
                if (schema == TransitdataProperties.ProtobufSchema.GTFS_TripUpdate) {
                    GtfsRealtime.FeedMessage feedMessage = GtfsRealtime.FeedMessage.parseFrom(msg.getData());

                    List<GtfsRealtime.TripUpdate> tripUpdates = feedMessage.getEntityList()
                            .stream()
                            .flatMap(
                                entity -> entity.hasTripUpdate() ? Stream.of(entity.getTripUpdate()) : Stream.empty()
                            ).collect(Collectors.toList());

                    for (GtfsRealtime.TripUpdate tu : tripUpdates) {
                        context = tripUpdatePipeline.handleMessage(context, tu);
                    }
                    /*tripUpdates.forEach(tu -> {
                        context = tripUpdatePipeline.handleMessage(context, tu);
                    });*/
                    /*if (tripUpdatePipeline != null) {
                        context = tripUpdatePipeline.handleMessage(context, feedMessage.getEntity(0).getTripUpdate());
                    }*/
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
