package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertEquals;

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
      c.storeAsBinary().enable();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assertEquals(StorageType.BINARY, ecm.getCache().getCacheConfiguration().memory().storageType());
   }

   public void testConfigCloning() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storageType(StorageType.BINARY);
      ConfigurationBuilder builder = new ConfigurationBuilder().read(c.build());
      Configuration clone = builder.build();
      assertEquals(StorageType.BINARY, clone.memory().storageType());
   }

   public void testConfigOverriding() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storageType(StorageType.BINARY);
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.defineConfiguration("newCache", new ConfigurationBuilder().read(c.build()).memory().storageType(StorageType.OBJECT).build());
      assertEquals(StorageType.OBJECT, ecm.getCache("newCache").getCacheConfiguration().memory().storageType());
   }

}
