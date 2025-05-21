package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.Iterator;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.server.test.api.HotRodTestClientDriver;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class JCacheOperations {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testJCacheOperations() throws IOException {
      Properties properties = new Properties();
      InetAddress serverAddress = SERVERS.getServerDriver().getServerAddress(0);
      properties.put(ConfigurationProperties.SERVER_LIST, serverAddress.getHostAddress() + ":11222");
      if (Boolean.getBoolean(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_NEWER_THAN_14)) {
         properties.put(
               ConfigurationProperties.CACHE_PREFIX + SERVERS.getMethodName()
                     + ConfigurationProperties.CACHE_CONFIGURATION_SUFFIX,
               HotRodTestClientDriver.toConfiguration("DIST_SYNC").toXMLString());
      } else {
         properties.put(ConfigurationProperties.CACHE_PREFIX + SERVERS.getMethodName()
               + ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX, "org.infinispan.DIST_SYNC");
      }

      doJCacheOperations(properties);
   }

   @Test
   public void testJCacheOperationsWildcards() throws IOException {
      Properties properties = new Properties();
      InetAddress serverAddress = SERVERS.getServerDriver().getServerAddress(0);
      properties.put(ConfigurationProperties.SERVER_LIST, serverAddress.getHostAddress() + ":11222");
      if (Boolean.getBoolean(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_NEWER_THAN_14)) {
         properties.put(
               ConfigurationProperties.CACHE_PREFIX + '[' + SERVERS.getMethodName().substring(0, 8) + "*]"
                     + ConfigurationProperties.CACHE_CONFIGURATION_SUFFIX,
               HotRodTestClientDriver.toConfiguration("DIST_SYNC").toXMLString());
      } else {
         properties.put(ConfigurationProperties.CACHE_PREFIX + '[' + SERVERS.getMethodName().substring(0, 8) + "*]"
               + ConfigurationProperties.CACHE_TEMPLATE_NAME_SUFFIX, "org.infinispan.DIST_SYNC");
      }
      doJCacheOperations(properties);
   }

   private void doJCacheOperations(Properties properties) throws IOException {
      File file = new File(String.format("target/test-classes/%s-hotrod-client.properties", SERVERS.getMethodName()));
      try (FileOutputStream fos = new FileOutputStream(file)) {
         properties.store(fos, null);
      }
      URI uri = file.toURI();
      CachingProvider provider = Caching.getCachingProvider();
      try (CacheManager cacheManager = provider.getCacheManager(uri, this.getClass().getClassLoader())) {
         Cache<String, String> cache = cacheManager.getCache(SERVERS.getMethodName());
         cache.put("k1", "v1");
         int size = getCacheSize(cache);
         assertEquals(1, size);
         assertEquals("v1", cache.get("k1"));
         cache.remove("k1");
         assertEquals(0, getCacheSize(cache));
      }
   }

   private int getCacheSize(Cache<String, String> cache) {
      int size = 0;
      for (Iterator<Cache.Entry<String, String>> it = cache.iterator(); it.hasNext(); it.next()) {
         ++size;
      }
      return size;
   }
}
