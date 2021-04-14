package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Dan Berindei
 * @since 12.1
 */
public class CloudEventsIntegrationIT {
   public static final String CACHE_ENTRIES_TOPIC = "cache-entries";

   public static KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/CloudEventsIntegration.xml")
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

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testSendCacheEntryEvent() {
      Properties kafkaProperties = new Properties();
      kafkaProperties.setProperty("bootstrap.servers", KAFKA.getBootstrapServers());
      kafkaProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      kafkaProperties.setProperty("group.id", CloudEventsIntegrationIT.class.getSimpleName());
      kafkaProperties.setProperty("auto.offset.reset", "earliest");
      KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
      try {
         assertTrue(kafkaConsumer.listTopics().containsKey(CACHE_ENTRIES_TOPIC));
         kafkaConsumer.subscribe(Collections.singleton(CACHE_ENTRIES_TOPIC));

         RemoteCacheManager rcm = SERVER_TEST.hotrod().createRemoteCacheManager();
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
      } finally {
         kafkaConsumer.close();
      }
   }
}
