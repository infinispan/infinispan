package org.infinispan.server.memcached;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Memcached server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedServerTest")
public class MemcachedServerTest extends AbstractInfinispanTest {

   void testValidateDefaultConfiguration() {
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(), cm ->
         Stoppable.useServer(new MemcachedServer(), ms -> {
            ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
            assertEquals(ms.getHost(), "127.0.0.1");
            assertEquals((int) ms.getPort(), 11211);
         }));
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   void testValidateInvalidExpiration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.expiration().lifespan(10);
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(config), cm ->
         Stoppable.useServer(new MemcachedServer(), ms -> {
            ms.start(new MemcachedServerConfigurationBuilder().cache("memcachedCache").build(), cm);
            fail("Server should not start when expiration is enabled");
         }));
   }

}
