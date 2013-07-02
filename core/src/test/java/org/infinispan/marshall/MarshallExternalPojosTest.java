package org.infinispan.marshall;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.PojoWithJBossExternalize;
import org.infinispan.commons.marshall.PojoWithSerializeWith;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "marshall.MarshallExternalPojosTest")
public class MarshallExternalPojosTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = MarshallExternalPojosTest.class.getName();

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalCfg1 = GlobalConfiguration.getClusteredDefault();
      GlobalConfiguration globalCfg2 = GlobalConfiguration.getClusteredDefault();
      CacheContainer cm1 = TestCacheManagerFactory.createCacheManager(globalCfg1);
      CacheContainer cm2 = TestCacheManagerFactory.createCacheManager(globalCfg2);
      registerCacheManager(cm1, cm2);
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      defineConfigurationOnAllManagers(CACHE_NAME, cfg);
      waitForClusterToForm(CACHE_NAME);
   }

   public void testReplicateJBossExternalizePojo(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateJBossExternalizePojo")
   public void testReplicateJBossExternalizePojoToNewJoiningNode(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(48, k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }

   public void testReplicateMarshallableByPojo(Method m) {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(17, k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateMarshallableByPojo")
   public void testReplicateMarshallableByPojoToNewJoiningNode(Method m) {
      PojoWithSerializeWith pojo = new PojoWithSerializeWith(85, k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }

   private void doReplicatePojo(Method m, Object o) {
      Cache cache1 = manager(0).getCache(CACHE_NAME);
      Cache cache2 = manager(1).getCache(CACHE_NAME);
      cache1.put(k(m), o);
      assertEquals(o, cache2.get(k(m)));
   }

   private void doReplicatePojoToNewJoiningNode(Method m, Object o) {
      Cache cache1 = manager(0).getCache(CACHE_NAME);
      EmbeddedCacheManager cm = createCacheManager();
      try {
         Cache cache3 = cm.getCache(CACHE_NAME);
         cache1.put(k(m), o);
         assertEquals(o, cache3.get(k(m)));
      } finally {
         cm.stop();
      }
   }

   private EmbeddedCacheManager createCacheManager() {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalCfg);
      Configuration cfg = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      cm.defineConfiguration(CACHE_NAME, cfg);
      return cm;
   }
}
