package org.infinispan.marshall;

import org.infinispan.config.Configuration;
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
      Configuration c = new Configuration().fluent().storeAsBinary().storeValuesAsBinary(false).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assert ecm.getCache().getConfiguration().isStoreAsBinary();
      assert ecm.getCache().getConfiguration().isStoreKeysAsBinary();
      assert !ecm.getCache().getConfiguration().isStoreValuesAsBinary();
   }

   public void testValuesOnly() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeKeysAsBinary(false).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assert ecm.getCache().getConfiguration().isStoreAsBinary();
      assert !ecm.getCache().getConfiguration().isStoreKeysAsBinary();
      assert ecm.getCache().getConfiguration().isStoreValuesAsBinary();
   }

   public void testBoth() {
      Configuration c = new Configuration().fluent().storeAsBinary().build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      assert ecm.getCache().getConfiguration().isStoreAsBinary();
      assert ecm.getCache().getConfiguration().isStoreKeysAsBinary();
      assert ecm.getCache().getConfiguration().isStoreValuesAsBinary();
   }

   public void testConfigCloning() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeKeysAsBinary(false).build();
      Configuration clone = c.clone();
      assert !clone.isStoreKeysAsBinary();
      assert clone.isStoreValuesAsBinary();
   }

   public void testConfigOverriding() {
      Configuration c = new Configuration().fluent().storeAsBinary().storeKeysAsBinary(false).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.defineConfiguration("newCache", c.clone().fluent().storeAsBinary().storeValuesAsBinary(false).storeKeysAsBinary(true).build());
      assert ecm.getCache("newCache").getConfiguration().isStoreAsBinary();
      assert ecm.getCache("newCache").getConfiguration().isStoreKeysAsBinary();
      assert !ecm.getCache("newCache").getConfiguration().isStoreValuesAsBinary();
   }
}
