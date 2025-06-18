package org.infinispan.client.hotrod.event;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test for receiving events in when the Server uses different storage types.
 */
@Test(groups = {"functional"}, testName = "client.hotrod.event.EventWithStorageTypeTest")
public class EventWithStorageTypeTest extends SingleHotRodServerTest {

   private StorageType storageType;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().storage(storageType);
      return TestCacheManagerFactory.createCacheManager(contextInitializer(), builder);
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new EventWithStorageTypeTest().storageType(StorageType.HEAP),
            new EventWithStorageTypeTest().storageType(StorageType.OFF_HEAP),
      };
   }

   private Object storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDataSCI.INSTANCE;
   }

   @Override
   protected String parameters() {
      return "storageType-" + storageType;
   }

   @Test
   public void testReceiveKeyValues() throws InterruptedException {
      RemoteCache<String, Person> cache = remoteCacheManager.getCache();
      EventListener listener = new EventListener();
      cache.addClientListener(listener);

      cache.put("1", new Person("John"));

      Object key = listener.getEventKey();

      assertNotNull(key);
      assertEquals("1", key);
   }

   @ClientListener
   @SuppressWarnings("unused")
   static class EventListener {
      private final BlockingQueue<String> eventsQueue = new LinkedBlockingQueue<>();

      Object getEventKey() throws InterruptedException {
         return eventsQueue.poll(5, TimeUnit.SECONDS);
      }

      @ClientCacheEntryCreated
      public void handleCreatedEvent(ClientCacheEntryCreatedEvent<String> event) {
         eventsQueue.add(event.getKey());
      }

      @ClientCacheEntryModified
      public void handleModifiedEvent(ClientCacheEntryModifiedEvent<String> event) {
         eventsQueue.add(event.getKey());
      }
   }

}
