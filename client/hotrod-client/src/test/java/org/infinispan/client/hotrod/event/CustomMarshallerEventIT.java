package org.infinispan.client.hotrod.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.testng.annotations.Test;

/**
 * Tests for remote listeners and custom marshaller.
 * The cache is not configured with any media type in particular, but the listener
 * should receive events that are to be unmarshalled by a specific user provided marshaller.
 */
@Test(groups = "functional", testName = "client.hotrod.event.CustomMarshallerEventIT")
public class CustomMarshallerEventIT extends SingleHotRodServerTest {

   @Override
   protected void setup() throws Exception {
      super.setup();
      // Make the custom marshaller available in the server. In standalone servers, this can done using a deployment jar.
      hotrodServer.setMarshaller(new IdMarshaller());
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.marshaller(new IdMarshaller());
      return new RemoteCacheManager(builder.build());
   }

   @Test
   public void testEventReceiveBasic() {
      RemoteCache<Id, Id> remoteCache = remoteCacheManager.getCache();
      final IdEventListener eventListener = new IdEventListener();
      remoteCache.addClientListener(eventListener);
      try {
         // Created events
         remoteCache.put(new Id(1), new Id(11));
         ClientCacheEntryCreatedEvent<Id> created = eventListener.pollEvent();
         assertEquals(new Id(1), created.getKey());
         remoteCache.put(new Id(2), new Id(22));
         created = eventListener.pollEvent();
         assertEquals(new Id(2), created.getKey());
         // Modified events
         remoteCache.put(new Id(1), new Id(111));
         ClientCacheEntryModifiedEvent<Id> modified = eventListener.pollEvent();
         assertEquals(new Id(1), modified.getKey());
         // Remove events
         remoteCache.remove(new Id(1));
         ClientCacheEntryRemovedEvent<Id> removed = eventListener.pollEvent();
         assertEquals(new Id(1), removed.getKey());
         remoteCache.remove(new Id(2));
         removed = eventListener.pollEvent();
         assertEquals(new Id(2), removed.getKey());
      } finally {
         remoteCache.removeClientListener(eventListener);
      }
   }

   @ClientListener
   public static class IdEventListener {

      BlockingQueue<ClientEvent> events = new ArrayBlockingQueue<>(128);

      @ClientCacheEntryCreated
      @ClientCacheEntryModified
      @ClientCacheEntryRemoved
      public void handleCreatedEvent(ClientEvent e) {
         events.add(e);
      }

      public <E extends ClientEvent> E pollEvent() {
         try {
            E event = (E) events.poll(10, TimeUnit.SECONDS);
            assertNotNull(event);
            return event;
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
      }
   }

   public static class IdMarshaller extends AbstractMarshaller {
      @Override
      protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
         Id obj = (Id) o;
         ByteBufferFactory factory = new ByteBufferFactoryImpl();
         return factory.newByteBuffer(new byte[]{obj.id}, 0, 1);
      }

      @Override
      public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
         return new Id(buf[0]);
      }

      @Override
      public boolean isMarshallable(Object o) {
         return true;
      }

      @Override
      public MediaType mediaType() {
         return MediaType.parse("application/x-custom-id");
      }
   }

   public static class Id implements Serializable {
      final byte id;

      public Id(int id) {
         this.id = (byte) id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Id other = (Id) o;
         return id == other.id;
      }

      @Override
      public int hashCode() {
         return id;
      }
   }

}
