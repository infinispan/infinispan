package org.infinispan.cloudevents.impl;

import static org.infinispan.factories.scopes.Scopes.GLOBAL;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.infinispan.cloudevents.configuration.CloudEventsGlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;

/**
 * Send messages to Kafka topics.
 *
 * @author Dan Berindei
 * @since 12
 */
@Scope(GLOBAL)
public class KafkaEventSenderImpl implements KafkaEventSender {

   @Inject GlobalConfiguration globalConfiguration;
   private KafkaProducer<byte[], byte[]> producer;

   @Start
   void start() {
      Properties kafkaProperties = new Properties();
      CloudEventsGlobalConfiguration cloudEventsGlobalConfiguration =
            globalConfiguration.module(CloudEventsGlobalConfiguration.class);

      kafkaProperties.put("bootstrap.servers", cloudEventsGlobalConfiguration.bootstrapServers());
      kafkaProperties.put("acks", String.valueOf(cloudEventsGlobalConfiguration.acks()));
      kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      producer = new KafkaProducer<>(kafkaProperties);
      connect(cloudEventsGlobalConfiguration);
   }

   private void connect(CloudEventsGlobalConfiguration cloudEventsGlobalConfiguration) {
      // Force the client to connect and receive metadata for the topics
      if (cloudEventsGlobalConfiguration.cacheEntryEventsEnabled()) {
         producer.partitionsFor(cloudEventsGlobalConfiguration.cacheEntriesTopic());
      }
      if (cloudEventsGlobalConfiguration.auditEventsEnabled()) {
         producer.partitionsFor(cloudEventsGlobalConfiguration.auditTopic());
      }
   }

   @Override
   public CompletionStage<Void> send(ProducerRecord<byte[], byte[]> record) {
      CompletableFuture<Void> cf = new CompletableFuture<>();
      producer.send(record, (metadata, exception) -> {
         if (exception != null) {
            cf.completeExceptionally(exception);
         } else {
            cf.complete(null);
         }
      });
      return cf;
   }
}
