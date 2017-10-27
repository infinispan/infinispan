package org.infinispan.server.test.cache.container;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.server.test.util.ITestUtils;
import org.infinispan.server.test.util.RemoteInfinispanMBeans;
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
@WithRunningServer({@RunningServer(name = OffHeapContainerIT.CONFIG_NAME)})
public class OffHeapContainerIT {

   public static final String CONFIG_NAME = "off-heap";
   private static RemoteCacheManager remoteCacheManager;

   @InfinispanResource(OffHeapContainerIT.CONFIG_NAME)
   RemoteInfinispanServer server1;

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

   @Test
   public void testRestEndpoint() throws Exception {
      String cacheName = "off-heap-default";
      CloseableHttpClient restClient = null;
      try {
         restClient = HttpClients.createDefault();
         RemoteInfinispanMBeans mbean = RemoteInfinispanMBeans.create(server1, OffHeapContainerIT.CONFIG_NAME, cacheName, "local");
         String restUrl = "http://" + mbean.server.getHotrodEndpoint().getInetAddress().getHostName() + ":8080"
               + mbean.server.getRESTEndpoint().getContextPath() + "/" + cacheName;

         String key = "key";
         byte[] value = "value".getBytes();
         HttpPut put = new HttpPut(restUrl + "/" + key);
         put.setEntity(new ByteArrayEntity(value, ContentType.APPLICATION_OCTET_STREAM));
         HttpResponse putResponse = restClient.execute(put);
         assertEquals(HttpStatus.SC_OK, putResponse.getStatusLine().getStatusCode());

         HttpGet get = new HttpGet(restUrl + "/" + key);
         get.addHeader("Accept", ContentType.APPLICATION_OCTET_STREAM.toString());
         HttpResponse getResponse = restClient.execute(get);
         assertEquals(HttpStatus.SC_OK, getResponse.getStatusLine().getStatusCode());
         assertArrayEquals(value, EntityUtils.toByteArray(getResponse.getEntity()));
      } finally {
         if (restClient != null) {
            restClient.close();
         }
      }
   }
}
