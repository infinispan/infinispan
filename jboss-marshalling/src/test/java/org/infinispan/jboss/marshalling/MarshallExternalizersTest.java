package org.infinispan.jboss.marshalling;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.core.JBossUserMarshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.MarshallExternalizersTest")
public class MarshallExternalizersTest extends MultipleCacheManagersTest {

   @Override
   public void createCacheManagers() throws Throwable {
      createCluster(globalConfigurationBuilder(), configBuilder(), 2);
      waitForClusterToForm();
   }

   public ConfigurationBuilder configBuilder() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
   }

   public GlobalConfigurationBuilder globalConfigurationBuilder() {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().marshaller(new JBossUserMarshaller());
      return globalBuilder;
   }

   public void testReplicateJBossExternalizePojo(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, TestingUtil.k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateJBossExternalizePojo")
   public void testReplicateJBossExternalizePojoToNewJoiningNode(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(48, TestingUtil.k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }

   private void doReplicatePojo(Method m, Object o) {
      Cache cache1 = manager(0).getCache();
      Cache cache2 = manager(1).getCache();
      cache1.put(k(m), o);
      assertEquals(o, cache2.get(k(m)));
   }

   private void doReplicatePojoToNewJoiningNode(Method m, Object o) {
      Cache cache1 = manager(0).getCache();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfigurationBuilder(), configBuilder());
      try {
         Cache cache3 = cm.getCache();
         cache1.put(k(m), o);
         assertEquals(o, cache3.get(k(m)));
      } finally {
         cm.stop();
      }
   }
}
