package org.infinispan.marshall;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

   public void testKeysOnly() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.storeAsBinary().enable().storeValuesAsBinary(false);
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().enabled();
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().storeKeysAsBinary();
      assert !ecm.getCache().getCacheConfiguration().storeAsBinary().storeValuesAsBinary();
   }

   public void testValuesOnly() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.storeAsBinary().enable().storeKeysAsBinary(false);
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().enabled();
      assert !ecm.getCache().getCacheConfiguration().storeAsBinary().storeKeysAsBinary();
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().storeValuesAsBinary();
   }

   public void testBoth() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.storeAsBinary().enable();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().enabled();
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().storeKeysAsBinary();
      assert ecm.getCache().getCacheConfiguration().storeAsBinary().storeValuesAsBinary();
   }

   public void testConfigCloning() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.storeAsBinary().enable().storeKeysAsBinary(false);
      ConfigurationBuilder builder = new ConfigurationBuilder().read(c.build());
      Configuration clone = builder.build();
      assert !clone.storeAsBinary().storeKeysAsBinary();
      assert clone.storeAsBinary().storeValuesAsBinary();
   }

   public void testConfigOverriding() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.storeAsBinary().enable().storeKeysAsBinary(false);
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.defineConfiguration("newCache", new ConfigurationBuilder().read(c.build()).storeAsBinary().storeValuesAsBinary(false).storeKeysAsBinary(true).build());
      assert ecm.getCache("newCache").getCacheConfiguration().storeAsBinary().enabled();
      assert ecm.getCache("newCache").getCacheConfiguration().storeAsBinary().storeKeysAsBinary();
      assert !ecm.getCache("newCache").getCacheConfiguration().storeAsBinary().storeValuesAsBinary();
   }
}
