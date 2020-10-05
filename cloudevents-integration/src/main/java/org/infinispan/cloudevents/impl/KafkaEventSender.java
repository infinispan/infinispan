package org.infinispan.cloudevents.impl;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Send messages to Kafka topics.
 *
 * @author Dan Berindei
 * @since 12
 */
public interface KafkaEventSender {
   CompletionStage<Void> send(ProducerRecord<byte[], byte[]> record);
}
