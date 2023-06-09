package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Ryan Emerson
 * @since 12.0
 */
public class HotRodAdmin {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

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
}
