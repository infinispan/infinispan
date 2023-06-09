package org.infinispan.server.functional.memcached;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class MemcachedOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   private static final AtomicInteger KCOUNTER = new AtomicInteger(0);

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testMemcachedOperations(ConnectionFactoryBuilder.Protocol protocol) throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient client = client(protocol);
      String k = k();
      client.set(k, 0, "v1").get(10, TimeUnit.SECONDS);
      assertEquals("v1", client.get(k));
   }

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testSetGetNewLineChars(ConnectionFactoryBuilder.Protocol protocol) throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient client = client(protocol);
      // make sure the set() finishes before retrieving the key
      String k = k();
      client.set(k, 0, "A\r\nA").get(10, TimeUnit.SECONDS);
      assertEquals("A\r\nA", client.get(k));
   }

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testFlush(ConnectionFactoryBuilder.Protocol protocol) throws Exception {
      MemcachedClient client = client(protocol);
      String k1 = k();
      String k2 = k();
      client.set(k1, 0, "v1");
      client.set(k2, 0, "v2").get(10, TimeUnit.SECONDS);
      assertTrue(client.flush().get());
      assertNull(client.get(k1));
      assertNull(client.get(k2));
   }

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testPutAsync(ConnectionFactoryBuilder.Protocol protocol) throws ExecutionException, InterruptedException {
      MemcachedClient client = client(protocol);
      String k = k();
      Future<Boolean> key1 = client.add(k, 10, "v1");
      assertTrue(key1.get());
      assertEquals("v1", client.get(k));
      assertNull(client.get(k()));
   }

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testNonExistentkey(ConnectionFactoryBuilder.Protocol protocol) {
      MemcachedClient client = client(protocol);
      assertNull(client.get(k()));
   }

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testConcurrentGets(ConnectionFactoryBuilder.Protocol protocol) throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient client = client(protocol);
      String k = k() + "-";
      int nKeys = 10;
      for (int i = 1; i < nKeys; ++i) {
         client.set(k + i, 0, "v-" + i);
      }
      // responses are sent ordered, waiting on the last one ensures that all the previous set() are completed!
      client.set(k + nKeys, 0, "v-" + nKeys).get(10, TimeUnit.SECONDS);

      List<GetFuture<Object>> getFutureList = new ArrayList<>(nKeys);
      for (int i = 1; i <= nKeys; ++i) {
         getFutureList.add(client.asyncGet(k + i));
      }

      for (int i = 1; i <= nKeys; ++i) {
         assertEquals("v-" + i, getFutureList.get(i - 1).get(10, TimeUnit.SECONDS));
      }
   }

   private MemcachedClient client(ConnectionFactoryBuilder.Protocol protocol) {
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      builder.setProtocol(protocol);
      return SERVERS.memcached().withClientConfiguration(builder).withPort(11221).get();
   }

   public static final String k() {
      return "k-" + KCOUNTER.incrementAndGet();
   }
}
