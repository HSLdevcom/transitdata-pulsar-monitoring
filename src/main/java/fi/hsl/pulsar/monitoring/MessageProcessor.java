package fi.hsl.pulsar.monitoring;

import fi.hsl.common.pulsar.IMessageHandler;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor implements IMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private Consumer<byte[]> consumer;

    public MessageProcessor(Consumer<byte[]> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void handleMessage(final Message msg) throws Exception {
        try {



            //Ack Pulsar message
            consumer.acknowledgeAsync(msg).thenRun(() -> {
                log.debug("Message acked");
            });
        }
        catch (Exception e) {
            log.error("Unknown error, existing app", e);
            throw e;
        }
    }

}
