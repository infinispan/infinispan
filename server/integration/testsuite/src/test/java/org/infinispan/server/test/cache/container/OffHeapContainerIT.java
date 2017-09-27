package org.infinispan.server.test.cache.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Basic off-heap tests
 *
 * @author vjuranek
 * @since 9.2
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "off-heap")})
public class OffHeapContainerIT {

   @InfinispanResource("off-heap")
   RemoteInfinispanServer server1;

   private static RemoteCacheManager remoteCacheManager;

   @Before
   public void setUp() {
      if (remoteCacheManager == null) {
         remoteCacheManager = ITestUtils.createCacheManager(server1);
      }
   }

   @Test
   public void testBasicOps() {
      RemoteCache<String, String> rc = remoteCacheManager.getCache("off-heap-bounded");
      rc.clear();

      rc.put("keyA", "A");
      rc.put("keyB", "B");
      rc.put("keyC", "C");
      rc.put("keyD", "D");
      assertEquals(3, rc.size());
      assertNull("Key A should be evicted from cache", rc.get("keyA"));
      assertEquals("B", rc.get("keyB"));
      assertEquals("C", rc.get("keyC"));
      assertEquals("D", rc.get("keyD"));
      assertFalse(rc.containsKey("keyA"));
      assertTrue(rc.containsKey("keyB"));

      rc.replace("keyB", "B", "BB");
      assertEquals("BB", rc.get("keyB"));

      rc.putIfAbsent("keyB", "BBB");
      assertEquals("BB", rc.get("keyB"));
      rc.putIfAbsent("keyA", "AA");
      assertEquals("AA", rc.get("keyA"));

      MetadataValue<String> meta = rc.getWithMetadata("keyA");
      assertTrue(rc.replaceWithVersion("keyA", "AAA", meta.getVersion()));
      assertEquals("AAA", rc.get("keyA"));
      rc.replace("keyA", "AAAA");
      assertFalse(rc.replaceWithVersion("keyA", "AAA", meta.getVersion()));
      assertEquals("AAAA", rc.get("keyA"));

      rc.put("keyA", "AAAAA", 10, TimeUnit.MILLISECONDS, 20, TimeUnit.MILLISECONDS);
      meta = rc.getWithMetadata("keyA");
      //assertEquals(10,meta.getLifespan());
      //assertEquals(20,meta.getMaxIdle());

      assertTrue(rc.containsKey("keyA"));
      //assertTrue(rc.containsValue("AAAAA"));

      rc.remove("keyA");
      assertFalse(rc.containsKey("keyA"));

      rc.clear();
      assertEquals(0, rc.size());
   }

   @Test
   public void testPutAndGetBulk() {
      RemoteCache<String, Integer> rc = remoteCacheManager.getCache("off-heap-default");
      rc.clear();

      Map<String, Integer> values = new HashMap<>();
      IntStream.range(0, 100).forEach(i -> values.put("key" + i, i));

      rc.putAll(values);
      assertEquals(100, rc.size());

      Set<String> keys = rc.keySet();
      assertEquals(100, keys.size());
      assertTrue(keys.contains("key0"));
      assertTrue(keys.contains("key50"));
      assertTrue(keys.contains("key99"));

      CloseableIterator<Map.Entry<Object, Object>> iter = rc.retrieveEntries(null, 100);
      //we cannot assert keys/values direcly as iterator returns items in random order
      Map<String, Integer> keyVal = new HashMap<>(100);
      while (iter.hasNext()) {
         Map.Entry<Object, Object> entry = iter.next();
         keyVal.put((String) entry.getKey(), (Integer) entry.getValue());
      }
      IntStream.range(0, 100).forEach(i -> {
         assertTrue(keyVal.containsKey("key" + i));
         assertEquals(new Integer(i), keyVal.get("key" + i));
      });
   }
}
