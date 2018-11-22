package fi.hsl.pulsar.monitoring.pipeline;

import com.google.transit.realtime.GtfsRealtime;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MonitoringPipeline implements IMessageHandler {
    private static final Logger log = LoggerFactory.getLogger(MonitoringPipeline.class);

    private TripUpdateCounter tripUpdatePipeline;
    private PipelineContext context = new PipelineContext();
    final ScheduledExecutorService scheduler;

    private MonitoringPipeline(int pollIntervalSecs) {
        tripUpdatePipeline = new TripUpdateCounter();

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
                    GtfsRealtime.TripUpdate tu = GtfsRealtime.TripUpdate.parseFrom(msg.getData());
                    if (tripUpdatePipeline != null) {
                        context = tripUpdatePipeline.handleMessage(context, tu);
                    }
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
