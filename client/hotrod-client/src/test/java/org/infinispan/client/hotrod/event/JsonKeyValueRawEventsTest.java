package org.infinispan.client.hotrod.event;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * Test for receiving events in different formats when using a converter.
 *
 * @since 9.4
 */
@Test(groups = {"functional",}, testName = "client.hotrod.event.JsonKeyValueRawEventsTest")
public class JsonKeyValueRawEventsTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      builder.encoding().value().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      return TestCacheManagerFactory.createCacheManager(contextInitializer(), builder);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   public void testReceiveKeyValuesAsJson() throws InterruptedException {
      BlockingQueue<KeyValuePair<String, String>> eventsQueue = new LinkedBlockingQueue<>();

      DataFormat jsonValues = DataFormat.builder().valueType(APPLICATION_JSON)
            .valueMarshaller(new UTF8StringMarshaller()).build();

      RemoteCache<String, Person> cache = remoteCacheManager.getCache();
      RemoteCache<String, String> jsonCache = cache.withDataFormat(jsonValues);

      jsonCache.addClientListener(new EventListener(eventsQueue, jsonCache.getDataFormat()));
      cache.put("1", new Person("John"));

      KeyValuePair<String, String> event = eventsQueue.poll(5, TimeUnit.SECONDS);

      assertNotNull(event);
      assertEquals(event.getKey(), "1");
      assertEquals(event.getValue(), "{\"_type\":\"org.infinispan.test.core.Person\",\"name\":\"John\",\"accepted_tos\":false,\"moneyOwned\":1.1,\"moneyOwed\":0.4,\"decimalField\":10.3,\"realField\":4.7}");
   }

   @ClientListener(converterFactoryName = "___eager-key-value-version-converter", useRawData = true)
   static class EventListener {

      private final Queue<KeyValuePair<String, String>> eventsQueue;
      private final DataFormat dataFormat;

      EventListener(Queue<KeyValuePair<String, String>> eventsQueue, DataFormat dataFormat) {
         this.eventsQueue = eventsQueue;
         this.dataFormat = dataFormat;
      }

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      public void handleCreatedModifiedEvent(ClientCacheEntryCustomEvent<byte[]> event) {
         eventsQueue.add(readEvent(event));
      }

      private KeyValuePair<String, String> readEvent(ClientCacheEntryCustomEvent<byte[]> event) {
         byte[] eventData = event.getEventData();
         ByteBuffer rawData = ByteBuffer.wrap(eventData);
         byte[] rawKey = readElement(rawData);
         byte[] rawValue = readElement(rawData);
         return new KeyValuePair<>(dataFormat.keyToObj(rawKey, null), dataFormat.valueToObj(rawValue, null));
      }

      private byte[] readElement(ByteBuffer buffer) {
         int length = UnsignedNumeric.readUnsignedInt(buffer);
         byte[] element = new byte[length];
         buffer.get(element);
         return element;
      }

   }

}
