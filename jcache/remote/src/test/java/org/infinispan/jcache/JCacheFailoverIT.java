package org.infinispan.jcache;

import java.net.URI;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;

import static javax.cache.configuration.FactoryBuilder.factoryOf;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.jcache.remote.JCachingProvider;
import org.jboss.arquillian.container.test.api.ContainerController;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;


/**
 * Tests for failover of JCache remote listeners
 *
 * @author gustavonalle
 * @since 8.0
 */
@RunWith(Arquillian.class)
public class JCacheFailoverIT {

   private static final String CONTAINER1 = "container-1";
   private static final String CONTAINER2 = "container-2";

   @ArquillianResource
   private ContainerController controller;

   private MutableConfiguration<String, String> createConfigurationWith(TrackingCacheEntryListener<String, String> listener) {
      return new MutableConfiguration<String, String>()
            .setTypes(String.class, String.class)
            .addCacheEntryListenerConfiguration(
                  new MutableCacheEntryListenerConfiguration<>(factoryOf(listener), null, true, true));
   }

   private CacheManager createCacheManagerWithTimeoutInMillis(int timeout) {
      Properties properties = new Properties();
      properties.put(ConfigurationProperties.SO_TIMEOUT, String.valueOf(timeout));

      return Caching.getCachingProvider().getCacheManager(URI.create(JCachingProvider.class.getName()), getClass().getClassLoader(), properties);
   }

   @Test
   public void testRemoteListener() {
      // Start a single server and listen to some events
      controller.start(CONTAINER1);
      assertTrue(controller.isStarted(CONTAINER1));

      TrackingCacheEntryListener<String, String> listener = new TrackingCacheEntryListener<>();
      MutableConfiguration<String, String> configuration = createConfigurationWith(listener);

      CacheManager cm = createCacheManagerWithTimeoutInMillis(3000);
      Cache<String, String> cache = cm.createCache("namedCache", configuration);

      cache.put("1", "value1");
      assertEquals(1, listener.getCreated());

      // Start a second server
      controller.start(CONTAINER2);
      assertTrue(controller.isStarted(CONTAINER2));

      // Generate more events
      cache.put("2", "value2");
      assertEquals(2, listener.getCreated());

      int beforeFailOver = listener.getCreated();

      // Kill server where listener was registered
      controller.kill(CONTAINER1);

      // Generate more events
      cache.put("3", "value3");
      cache.put("4", "value4");
      cache.put("5", "value5");

      // Check events continue to arrive
      assertTrue(listener.getCreated() > beforeFailOver);
   }

}
