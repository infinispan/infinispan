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
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;
import net.spy.memcached.internal.GetFuture;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class MemcachedOperations {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

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
      // Wait for all sets to complete. The client distributes keys across servers via consistent
      // hashing, so waiting on a single key only guarantees completion on that key's server.
      List<Future<Boolean>> setFutures = new ArrayList<>(nKeys);
      for (int i = 1; i <= nKeys; ++i) {
         setFutures.add(client.set(k + i, 0, "v-" + i));
      }
      for (Future<Boolean> f : setFutures) {
         f.get(10, TimeUnit.SECONDS);
      }

      List<GetFuture<Object>> getFutureList = new ArrayList<>(nKeys);
      for (int i = 1; i <= nKeys; ++i) {
         getFutureList.add(client.asyncGet(k + i));
      }

      for (int i = 1; i <= nKeys; ++i) {
         assertEquals("v-" + i, getFutureList.get(i - 1).get(10, TimeUnit.SECONDS));
      }
   }

   @ParameterizedTest
   @EnumSource(ConnectionFactoryBuilder.Protocol.class)
   public void testImplicitNoAuthMemcached(ConnectionFactoryBuilder.Protocol protocol)  {
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      builder.setProtocol(protocol);
      MemcachedClient client = SERVERS.memcached().withClientConfiguration(builder).withPort(11222).get();
      try {
         client.get(k());
         if (protocol == ConnectionFactoryBuilder.Protocol.TEXT) {
            throw new RuntimeException("Implicit memcached text request should have failed");
         }
      } catch (OperationTimeoutException e) {
         if (protocol == ConnectionFactoryBuilder.Protocol.BINARY) {
            throw new RuntimeException("Implicity memcached binary request should have succeeded", e);
         }
      }
   }

   private MemcachedClient client(ConnectionFactoryBuilder.Protocol protocol) {
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      builder.setProtocol(protocol);
      return SERVERS.memcached().withClientConfiguration(builder).withPort(11221).get();
   }

   public static String k() {
      return "k-" + KCOUNTER.incrementAndGet();
   }
}
