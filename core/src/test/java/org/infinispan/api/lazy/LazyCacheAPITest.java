package org.infinispan.api.lazy;

import java.lang.reflect.Method;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Cache API test with lazy deserialization turned on.
 *
 * @author Galder Zamarre�o
 * @since 4.1
 */
@Test(groups = "functional", testName = "api.lazy.LazyCacheAPITest")
public class LazyCacheAPITest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.memory().storage(StorageType.HEAP).encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(true, TestDataSCI.INSTANCE);
      cm.defineConfiguration("lazy-cache-test", c.build());
      cache = cm.getCache("lazy-cache-test");
      return cm;
   }

   public void testReplace(Method m) {
      Person key = new Person(m.getName());
      cache.put(key, "1");
      assert "1".equals(cache.get(new Person(m.getName())));
      Object oldValue = cache.replace(new Person(m.getName()), "2");
      assert "1".equals(oldValue);
      assert "2".equals(cache.get(new Person(m.getName())));
   }

   public void testReplaceWithOld(Method m) {
      Person key = new Person(m.getName());
      cache.put(key, "1");
      assert "1".equals(cache.get(new Person(m.getName())));
      assert !cache.replace(new Person(m.getName()), "99", "2");
      assert cache.replace(new Person(m.getName()), "1", "2");

      key = new Person(m.getName() + "-withCustomValue");
      Person v1 = new Person("value1");
      cache.put(key, v1);
      assert v1.equals(cache.get(key));
      Person v99 = new Person("value99");
      Person v2 = new Person("value2");
      assert !cache.replace(key, v99, v2);
      assert cache.replace(key, v1, v2);
   }
}
