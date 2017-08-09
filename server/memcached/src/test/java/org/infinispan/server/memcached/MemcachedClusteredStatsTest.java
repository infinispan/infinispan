package org.infinispan.server.memcached;

import java.util.Properties;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jmx.MBeanServerLookup;
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

   private String jmxDomain = MemcachedClusteredStatsTest.class.getSimpleName();

   private MBeanServerLookup mbeanServerLookup = new ProvidedMBeanServerLookup(MBeanServerFactory.createMBeanServer());

   @Override
   public EmbeddedCacheManager createCacheManager(int index) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      // Per-thread mbean server won't work here because the registration will
      // happen in the 'main' thread and the remote call will try to resolve it
      // in a lookup instance associated with an 'OOB-' thread.
      return TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain,
            jmxDomain + "-" + index, true,
            false, builder, mbeanServerLookup);
   }

   public void testSingleConnectionPerServer() throws MalformedObjectNameException, AttributeNotFoundException,
         MBeanException, ReflectionException, InstanceNotFoundException {
      MBeanServer mBeanServer = mbeanServerLookup.getMBeanServer(null);
      ConnectionStatsTest.testGlobalConnections(jmxDomain + "-0", "Memcached-" + jmxDomain, 2,
            mBeanServer);
   }

   class ProvidedMBeanServerLookup implements MBeanServerLookup {
      private final MBeanServer mbeanServer;

      ProvidedMBeanServerLookup(MBeanServer mbeanServer) {
         this.mbeanServer = mbeanServer;
      }

      @Override
      public MBeanServer getMBeanServer(Properties properties) {
         return mbeanServer;
      }
   }

}
