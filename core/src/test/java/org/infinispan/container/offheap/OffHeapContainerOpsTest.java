package org.infinispan.container.offheap;


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.TimeUnit;

import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

/**
 * @author vjuranek
 * @since 9.2
 */
@Test(groups = "functional", testName = "container.offheap.OffHeapContainerOpsTest")
public class OffHeapContainerOpsTest extends SingleCacheManagerTest {

   protected DataContainer container;
   protected ControlledTimeService timeService;

   private WrappedBytes key = new WrappedByteArray("key".getBytes());
   private WrappedBytes value = new WrappedByteArray("value".getBytes());

   @Override
   protected void setup() throws Exception {
      super.setup();
      container = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache().getDataContainer();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = getDefaultStandaloneCacheConfig(false);
      cb.memory().storageType(StorageType.OFF_HEAP);
      cb.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).storeName(getClass().getSimpleName());
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);

      timeService = new ControlledTimeService();
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);

      cm.defineConfiguration("test", cb.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testPutAndGet() {
      container.put(key, value, null);
      InternalCacheEntry entry = container.get(key);
      assertNotNull("Container returned  null for key" + key, entry);
      assertTrue("Key obtained from container is not equal to original key", key.equalsWrappedBytes((WrappedBytes) entry.getKey()));
   }

   public void testContainsKey() {
      container.put(key, value, null);
      assertTrue("Container should contain key " + key, container.containsKey(key));
   }

   public void testSize() {
      container.put(key, value, null);
      assertEquals(1, container.size());
   }

   public void testSizeIncludingExpired() {
      cache.put(key, value, 1, TimeUnit.SECONDS);
      timeService.advance(10_000);
      assertEquals(0, container.size());
      assertEquals("Expired entry not included into container size", 1, container.sizeIncludingExpired());
   }

   public void testRemove() {
      container.put(key, value, null);
      assertEquals(1, container.size());
      container.remove(key);
      assertEquals(0, container.size());
   }

   public void testClear() {
      container.put(key, value, null);
      container.clear();
      assertEquals("Wrong cache size",0, container.size());
   }

   public void testEviction() {
      container.put(key, value, null);
      container.evict(key);
      assertEquals(0, container.size());
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      MarshalledEntry entry = loader.load(key);
      assertNotNull("Key " + key + " exists in data container", entry);
      assertEquals(key, entry.getKey());
   }

   @Test(enabled = false) //ISPN-8385
   public void testPeek() {
      cache.put(key, value, 10, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
      timeService.advance(900);
      assertEquals(value, container.peek(key));
      timeService.advance(300);
      assertEquals("Entry should be expired", 0, container.size());
   }

   @Test(enabled = false) //ISPN-8385
   public void testPutViaCacheGetViaContainer() {
      cache.put(key, value);
      InternalCacheEntry entry = container.get(key);
      assertNotNull("Container returned  null for key" + key, entry);
      assertTrue("Key obtained from container is not equal to original key", key.equalsWrappedBytes((WrappedBytes) entry.getKey()));
   }
}
