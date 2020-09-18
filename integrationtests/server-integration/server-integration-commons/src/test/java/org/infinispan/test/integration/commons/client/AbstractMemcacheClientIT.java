package org.infinispan.test.integration.commons.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.junit.After;
import org.junit.Test;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;

public class AbstractMemcacheClientIT {

   private MemcachedClient client;

   @Test
   public void testMemcachedClient() {
      client = createMemcachedClient();
      client.add("a", 0, "a");
      assertEquals("a", client.get("a"));
   }

   @After
   public void cleanUp() {
      if (client != null)
         client.shutdown();
   }

   public static MemcachedClient createMemcachedClient() {
      DefaultConnectionFactory d = new DefaultConnectionFactory() {
         @Override
         public long getOperationTimeout() {
            return 60_000;
         }
      };
      try {
         return new MemcachedClient(d, Collections.singletonList(new InetSocketAddress("127.0.0.1", 11221)));
      } catch (IOException e) {
         throw new IllegalStateException("Cannot create MemcachedClient", e);
      }
   }
}
