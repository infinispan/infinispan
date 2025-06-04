package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.StoreAsBinaryConfigTest")
public class StoreAsBinaryConfigTest extends AbstractInfinispanTest {

   EmbeddedCacheManager ecm;

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(ecm);
      ecm = null;
   }

   public void testBackwardCompatibility() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storage(StorageType.BINARY);
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assertEquals(StorageType.BINARY, ecm.getCache().getCacheConfiguration().memory().storage());
   }

   public void testConfigCloning() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storage(StorageType.BINARY);
      ConfigurationBuilder builder = new ConfigurationBuilder().read(c.build(), Combine.DEFAULT);
      Configuration clone = builder.build();
      assertEquals(StorageType.BINARY, clone.memory().storage());
   }

   public void testConfigOverriding() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storage(StorageType.BINARY);
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.defineConfiguration("newCache", new ConfigurationBuilder().read(c.build(), Combine.DEFAULT).memory().storage(StorageType.HEAP).build());
      assertEquals(StorageType.HEAP, ecm.getCache("newCache").getCacheConfiguration().memory().storage());
   }

}
