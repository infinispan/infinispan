package org.infinispan.client.hotrod.event;

import static org.testng.AssertJUnit.assertNotNull;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
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
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.util.KeyValuePair;
import org.testng.annotations.Test;

/**
 * Test for listener with EagerKeyValueVersionConverter
 *
 * @since 10.0
 */
@Test(groups = {"functional"}, testName = "client.hotrod.event.EagerKeyValueConverterTest")
public class EagerKeyValueConverterTest extends SingleHotRodServerTest {

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   public void testWriteMap() throws InterruptedException {
      BlockingQueue<KeyValuePair<String, Person>> eventsQueue = new LinkedBlockingQueue<>();

      RemoteCache<String, Person> cache = remoteCacheManager.getCache();

      cache.addClientListener(new EventListener(eventsQueue, cache.getDataFormat()));

      Map<String, Person> data = new HashMap<>();
      data.put("1", new Person("John"));
      data.put("2", new Person("Mary"));
      data.put("3", new Person("George"));

      cache.putAll(data);

      KeyValuePair<String, Person> event = eventsQueue.poll(5, TimeUnit.SECONDS);

      assertNotNull(event);
   }


   @ClientListener(converterFactoryName = "___eager-key-value-version-converter", useRawData = true)
   static class EventListener {

      private final Queue<KeyValuePair<String, Person>> eventsQueue;
      private final DataFormat dataFormat;

      EventListener(Queue<KeyValuePair<String, Person>> eventsQueue, DataFormat dataFormat) {
         this.eventsQueue = eventsQueue;
         this.dataFormat = dataFormat;
      }

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      public void handleCreatedModifiedEvent(ClientCacheEntryCustomEvent<byte[]> event) {
         eventsQueue.add(readEvent(event));
      }

      private KeyValuePair<String, Person> readEvent(ClientCacheEntryCustomEvent<byte[]> event) {
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
