package org.infinispan.marshall;

import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.PojoWithSerializeWith;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.MarshallExternalizersTest")
public class MarshallExternalizersTest extends MultipleCacheManagersTest {


   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, configBuilder(), 2);
      waitForClusterToForm();
   }

   private ConfigurationBuilder configBuilder() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
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

   protected void doReplicatePojo(Method m, Object o) {
      Cache cache1 = manager(0).getCache();
      Cache cache2 = manager(1).getCache();
      cache1.put(k(m), o);
      assertEquals(o, cache2.get(k(m)));
   }

   protected void doReplicatePojoToNewJoiningNode(Method m, Object o) {
      Cache cache1 = manager(0).getCache();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(TestDataSCI.INSTANCE, configBuilder());
      try {
         Cache cache3 = cm.getCache();
         cache1.put(k(m), o);
         assertEquals(o, cache3.get(k(m)));
      } finally {
         cm.stop();
      }
   }
}
