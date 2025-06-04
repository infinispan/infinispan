package org.infinispan.marshall;

import org.infinispan.commons.marshall.MarshallingException;
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
      c.invocationBatching().enable().memory().storage(StorageType.BINARY);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      cache = cm.getCache();
      return cm;
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNonMarshallableValue() {
      cache.put("Hello", new Object());
   }

   @Test(expectedExceptions = MarshallingException.class)
   public void testNonMarshallableKey() {
      cache.put(new Object(), "Hello");
   }

}
