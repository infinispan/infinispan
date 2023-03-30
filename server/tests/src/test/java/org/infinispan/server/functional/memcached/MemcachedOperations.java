package org.infinispan.server.functional.memcached;

import static org.infinispan.server.test.junit4.InfinispanServerTestMethodRule.k;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class MemcachedOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final ConnectionFactoryBuilder.Protocol protocol;

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>();
      params.add(new Object[]{ConnectionFactoryBuilder.Protocol.TEXT});
      params.add(new Object[]{ConnectionFactoryBuilder.Protocol.BINARY});
      return params;
   }

   public MemcachedOperations(ConnectionFactoryBuilder.Protocol protocol) {
      this.protocol = protocol;
   }

   @Test
   public void testMemcachedOperations() throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient client = client();
      String k = k();
      client.set(k, 0, "v1").get(10, TimeUnit.SECONDS);
      assertEquals("v1", client.get(k));
   }

   @Test
   public void testSetGetNewLineChars() throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient client = client();
      // make sure the set() finishes before retrieving the key
      String k = k();
      client.set(k, 0, "A\r\nA").get(10, TimeUnit.SECONDS);
      assertEquals("A\r\nA", client.get(k));
   }

   @Test
   public void testFlush() throws Exception {
      MemcachedClient client = client();
      String k1 = k();
      String k2 = k();
      client.set(k1, 0, "v1");
      client.set(k2, 0, "v2").get(10, TimeUnit.SECONDS);
      assertTrue(client.flush().get());
      assertNull(client.get(k1));
      assertNull(client.get(k2));
   }

   @Test
   public void testPutAsync() throws ExecutionException, InterruptedException {
      MemcachedClient client = client();
      String k = k();
      Future<Boolean> key1 = client.add(k, 10, "v1");
      assertTrue(key1.get());
      assertEquals("v1", client.get(k));
      assertNull(client.get(k()));
   }

   @Test
   public void testNonExistentkey() {
      MemcachedClient client = client();
      assertNull(client.get(k()));
   }

   @Test
   public void testConcurrentGets() throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient client = client();
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

   private MemcachedClient client() {
      ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
      builder.setProtocol(protocol);
      return SERVER_TEST.memcached().withClientConfiguration(builder).withPort(11221).get();
   }
}
