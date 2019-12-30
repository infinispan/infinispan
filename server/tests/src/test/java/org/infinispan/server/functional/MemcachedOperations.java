package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import net.spy.memcached.MemcachedClient;

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
   public void testMemcachedOperations() {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      cache.set("k1", 0, "v1");
      assertEquals("v1", cache.get("k1"));
   }

   @Test
   public void testSetGetNewLineChars() {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      cache.set("a",0, "A\r\nA");
      assertEquals("A\r\nA", cache.get("a"));
   }

   @Test
   public void testFlush() throws Exception {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      cache.set("k1", 0,  "v1");
      cache.set("k2", 0, "v2");
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
   public void testNonExistentkey() throws ExecutionException, InterruptedException {
      MemcachedClient cache = SERVER_TEST.getMemcachedClient();
      assertNull(cache.get("nonExistentkey"));
   }

}
