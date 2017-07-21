package org.infinispan.marshall;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests just enabling marshalled values on keys and not values, and vice versa.
 *
 * @author Manik Surtani
 */
@Test(groups = "functional", testName = "marshall.MarshalledValuesFineGrainedTest")
public class MarshalledValuesFineGrainedTest extends AbstractInfinispanTest {
   EmbeddedCacheManager ecm;
   final CustomClass key = new CustomClass("key");
   final CustomClass value = new CustomClass("value");

   @AfterMethod
   public void cleanup() {
      TestingUtil.killCacheManagers(ecm);
      ecm = null;
   }

   public void testStoreAsBinaryOnBoth() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.memory().storageType(StorageType.BINARY).build();
      ecm = TestCacheManagerFactory.createCacheManager(c);
      ecm.getCache().put(key, value);
      DataConversion keyDataConversion = ecm.getCache().getAdvancedCache().getKeyDataConversion();
      DataConversion valueDataConversion = ecm.getCache().getAdvancedCache().getValueDataConversion();

      DataContainer<?, ?> dc = ecm.getCache().getAdvancedCache().getDataContainer();

      InternalCacheEntry entry = dc.iterator().next();
      Object key = entry.getKey();
      Object value = entry.getValue();

      assertTrue(key instanceof WrappedBytes);
      assertEquals(keyDataConversion.fromStorage(key), this.key);

      assertTrue(value instanceof WrappedBytes);
      assertEquals(valueDataConversion.fromStorage(value), this.value);
   }
}
