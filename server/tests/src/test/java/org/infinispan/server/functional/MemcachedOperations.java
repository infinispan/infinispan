package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class MemcachedOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testMemcachedOperations() throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      cache.set("k1", 0, "v1").get(10, TimeUnit.SECONDS);
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testSetGetNewLineChars() throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      // make sure the set()) finishes before retrieving the key
      cache.set("a",0, "A\r\nA").get(10, TimeUnit.SECONDS);
      assertEquals("A\r\nA", cache.get("a"));
   }

   @Test
   public void testFlush() throws Exception {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      cache.set("k1", 0, "v1");
      cache.set("k2", 0, "v2").get(10, TimeUnit.SECONDS);
      assertTrue(cache.flush().get());
      assertNull(cache.get("k1"));
      assertNull(cache.get("k2"));
   }

   @Test
   public void testPutAsync() throws ExecutionException, InterruptedException {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      Future<Boolean> key1 = cache.add("k1", 10, "v1");
      assertTrue(key1.get());
      assertEquals("v1", cache.get("k1"));
      assertNull(cache.get("nonExistentkey"));
   }

   @Test
   public void testNonExistentkey() {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      assertNull(cache.get("nonExistentkey"));
   }

   @Test
   public void testConcurrentGets() throws ExecutionException, InterruptedException, TimeoutException {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      int nKeys = 10;
      for (int i = 0; i < nKeys - 1; ++i) {
         cache.set("key-" + i, 0, "value-" + i);
      }
      // responses are sent ordered, waiting on the last one ensures that all the previous set() are completed!
      cache.set("key-" + (nKeys - 1), 0, "value-" + (nKeys - 1)).get(10, TimeUnit.SECONDS);

      List<GetFuture<Object>> getFutureList = new ArrayList<>(nKeys);
      for (int i = 0; i < nKeys; ++i) {
         getFutureList.add(cache.asyncGet("key-" + i));
      }

      for (int i = 0; i < nKeys; ++i) {
         assertEquals("value-" + i, getFutureList.get(i).get(10, TimeUnit.SECONDS));
      }
   }

}
