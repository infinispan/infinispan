package org.infinispan.all.remote;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(Arquillian.class)
public class RemoteAllTest {

   private static RemoteCacheManager rcm = null;

   @BeforeClass
   public static void beforeTest() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer()
            .host("127.0.0.1")
            .port(11222)
            .protocolVersion(ConfigurationProperties.PROTOCOL_VERSION_20);
      rcm = new RemoteCacheManager(builder.build());
   }

   @AfterClass
   public static void cleanUp() {
      if (rcm != null)
         rcm.stop();
   }

   @Test
   public void testBasicHotRodPutGetRemoteAll() {
      RemoteCache<String, String> c1 = rcm.getCache("default");
      c1.put("key1", "value1");
      assertEquals("value1", c1.get("key1"));
   }

   /**
    * Test for remote cache store in the context of uber-jars.
    *
    * In order to make remote cache store working, both infinispan-embedded
    * and infinispan-remote libraries has to be on classpath.
    *
    * The started infinispan server is a remote store for us here.
    */
   @Test
   public void testRemoteCacheStoreAll() {

      org.infinispan.configuration.cache.ConfigurationBuilder builder =
            new org.infinispan.configuration.cache.ConfigurationBuilder();

      builder.clustering().cacheMode(CacheMode.LOCAL)
            .persistence().passivation(true)
            .addStore(RemoteStoreConfigurationBuilder.class)
            // configured in clustered-indexing.xml used by Arquillian in our tests here
            .remoteCacheName("cache-as-remote-store")
            .purgeOnStartup(false).preload(true)
            .addServer()
            .host("127.0.0.1")
            .port(11222);

      GlobalConfiguration globalConfiguration = GlobalConfigurationBuilder
            .defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
            .transport().nodeName("node1_embedded")
            .build();

      DefaultCacheManager manager = new DefaultCacheManager(globalConfiguration);

      manager.defineConfiguration("cache-with-remote-store", builder.build());
      Cache<Object, Object> cache = manager.getCache("cache-with-remote-store");
      
      cache.put("key1", "value1");
      cache.put("key2", "value2");

      assertEquals("value1", cache.get("key1"));
      assertEquals("value2", cache.get("key2"));

      cache.stop();
      cache.start();

      // data survived?
      assertEquals("value1", cache.get("key1"));
      assertEquals("value2", cache.get("key2"));
   }
}
