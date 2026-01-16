package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.infinispan.testing.Exceptions;
import org.junit.jupiter.api.Test;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class HotRodAdmin {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @Test
   public void testCreateDeleteCache() {
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      String cacheName = "testCreateDeleteCache";
      String config = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      RemoteCache<String, String> cache = rcm.administration().createCache(cacheName, new StringConfiguration(config));
      cache.put("k", "v");
      assertNotNull(cache.get("k"));

      rcm.administration().removeCache(cacheName);
      assertNull(rcm.getCache(cacheName));
   }

   @Test
   public void testCreateDeleteCacheFragment() {
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      String cacheName = "testCreateDeleteCacheFragment";
      String config = String.format("<distributed-cache name=\"%s\"/>", cacheName);
      RemoteCache<String, String> cache = rcm.administration().createCache(cacheName, new StringConfiguration(config));
      cache.put("k", "v");
      assertNotNull(cache.get("k"));

      rcm.administration().removeCache(cacheName);
      assertNull(rcm.getCache(cacheName));
   }

   @Test
   public void testCreateDeleteTemplate() {
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      String templateName = "template";
      String template = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", templateName);
      rcm.administration().createTemplate(templateName, new StringConfiguration(template));
      RemoteCache<String, String> cache = rcm.administration().createCache("testCreateDeleteTemplate", templateName);
      cache.put("k", "v");
      assertNotNull(cache.get("k"));

      rcm.administration().removeTemplate(templateName);
      Exceptions.expectException(HotRodClientException.class, () -> rcm.administration().createCache("anotherCache", templateName));
   }

   @Test
   public void testCreateDeleteTemplateFragment() {
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      String templateName = "templateFragment";
      String template = String.format("<distributed-cache name=\"%s\"/>", templateName);
      rcm.administration().createTemplate(templateName, new StringConfiguration(template));
      RemoteCache<String, String> cache = rcm.administration().createCache("testCreateDeleteTemplateFragment", templateName);
      cache.put("k", "v");
      assertNotNull(cache.get("k"));

      rcm.administration().removeTemplate(templateName);
      Exceptions.expectException(HotRodClientException.class, () -> rcm.administration().createCache("anotherCache", templateName));
   }

   @Test
   public void testAlias() {
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      RemoteCacheManagerAdmin admin = rcm.administration();
      RemoteCache<String, String> wuMing1 = admin.createCache("wu-ming-1",
            new StringConfiguration("""
                  {
                     "distributed-cache" : {
                        "aliases": ["wu-ming"]
                     }
                  }
                  """));
      RemoteCache<String, String> wuMing2 = admin.createCache("wu-ming-2",
            new StringConfiguration("""
                  {"distributed-cache" : {}}
                  """));
      RemoteCache<String, String> wuMing = rcm.getCache("wu-ming");

      // Write different data in the two backing caches
      wuMing1.put("key", "v1");
      wuMing2.put("key", "v2");

      assertEquals("v1", wuMing.get("key"));

      // Flip the alias
      admin.assignAlias("wu-ming", "wu-ming-2");

      assertEquals("v2", wuMing.get("key"));
   }
}
