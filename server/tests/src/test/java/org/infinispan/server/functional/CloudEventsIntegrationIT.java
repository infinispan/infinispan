package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Dan Berindei
 * @since 12.1
 */
@Disabled("The cp-kafka image is not available.")
@Tag("embedded")

public class CloudEventsIntegrationIT {
   public static final String CACHE_ENTRIES_TOPIC = "cache-entries";

   public static KafkaContainer KAFKA =
         new KafkaContainer(DockerImageName.parse("quay.io/cloudservices/cp-kafka:5.4.3")
                                           .asCompatibleSubstituteFor("confluentinc/cp-kafka"));

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/CloudEventsIntegration.xml")
               .numServers(1)
               .runMode(ServerRunMode.EMBEDDED)
               .addListener(new InfinispanServerListener() {
                  @Override
                  public void before(InfinispanServerDriver driver) {
                     KAFKA.start();
                     driver.getConfiguration().properties()
                           .setProperty("kafka.bootstrap.servers", KAFKA.getBootstrapServers());
                  }

                  @Override
                  public void after(InfinispanServerDriver driver) {
                     KAFKA.stop();
                  }
               })
               .build();

   @Test
   public void testSendCacheEntryEvent() {
      Properties kafkaProperties = new Properties();
      kafkaProperties.setProperty("bootstrap.servers", KAFKA.getBootstrapServers());
      kafkaProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaProperties.setProperty("group.id", CloudEventsIntegrationIT.class.getSimpleName());
      kafkaProperties.setProperty("auto.offset.reset", "earliest");
      try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProperties)) {
         assertTrue(kafkaConsumer.listTopics().containsKey(CACHE_ENTRIES_TOPIC));
         kafkaConsumer.subscribe(Collections.singleton(CACHE_ENTRIES_TOPIC));

         RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
         RemoteCache<String, String> cache = rcm.getCache("default");
         assertNotNull(cache);
         cache.put("k1", "v1");
         assertEquals("v1", cache.get("k1"));

         ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(1));
         assertEquals(1, records.count());

         ConsumerRecord<byte[], byte[]> eventRecord = records.iterator().next();
         Json expectedKeyJson = Json.object().set("_type", "string").set("_value", "k1");
         Json actualKeyJson = Json.read(new String(eventRecord.key()));
         assertEquals(expectedKeyJson, actualKeyJson);
      }
   }
}
