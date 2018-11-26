package fi.hsl.pulsar.monitoring;

import com.typesafe.config.Config;
import fi.hsl.common.pulsar.IMessageHandler;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.pulsar.monitoring.pipeline.MonitoringPipeline;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor implements IMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private Consumer<byte[]> consumer;
    private MonitoringPipeline pipeline;

    public MessageProcessor(PulsarApplicationContext context) {
        this.consumer = context.getConsumer();
        pipeline = MonitoringPipeline.newPipeline(context.getConfig());
    }

    @Override
    public void handleMessage(final Message msg) throws Exception {
        try {
            pipeline.handleMessage(msg);

            //Ack Pulsar message
            consumer.acknowledgeAsync(msg).thenRun(() -> {
                log.debug("Message acked");
            });
        }
        catch (Exception e) {
            log.error("Unknown error, existing app", e);
            pipeline.close();
            throw e;
        }
    }

}
