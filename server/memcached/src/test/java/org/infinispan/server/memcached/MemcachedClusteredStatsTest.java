package org.infinispan.server.memcached;

import javax.management.JMException;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.MBeanServerLookupProvider;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests whether statistics of clustered Memcached instances are calculated correctly.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedClusteredStatsTest")
public class MemcachedClusteredStatsTest extends MemcachedMultiNodeTest {

   private static final String JMX_DOMAIN = MemcachedClusteredStatsTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = MBeanServerLookupProvider.create();

   @Override
   public EmbeddedCacheManager createCacheManager(int index) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      return TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(JMX_DOMAIN,
            JMX_DOMAIN + "-" + index, true, false, builder, mBeanServerLookup);
   }

   public void testSingleConnectionPerServer() throws Exception {
      ObjectName objectName = new ObjectName(String.format("%s-0:type=Server,component=Transport,name=Memcached-%s-%d",
            JMX_DOMAIN, getClass().getSimpleName(), servers.get(0).getPort()));

      // Now verify that via JMX as well, these stats are also as expected
      eventuallyEquals(2, () -> {
         try {
            return mBeanServerLookup.getMBeanServer().getAttribute(objectName, "NumberOfGlobalConnections");
         } catch (JMException e) {
            log.debug("Exception encountered", e);
         }
         return 0;
      });
   }
}
