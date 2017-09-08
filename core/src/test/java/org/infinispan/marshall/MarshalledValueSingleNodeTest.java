package org.infinispan.marshall;

import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.MarshalledValueSingleNodeTest")
public class MarshalledValueSingleNodeTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.invocationBatching().enable().memory().storageType(StorageType.BINARY);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cache = cm.getCache();
      return cm;
   }

   @Test(expectedExceptions = NotSerializableException.class)
   public void testNonSerializableValue() {
      cache.put("Hello", new Object());
   }

   @Test(expectedExceptions = NotSerializableException.class)
   public void testNonSerializableKey() {
      cache.put(new Object(), "Hello");
   }

}
