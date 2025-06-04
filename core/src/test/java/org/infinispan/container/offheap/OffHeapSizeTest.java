package org.infinispan.container.offheap;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.ByteQuantity;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.impl.KeyValueMetadataSizeCalculator;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
@Test(groups = "functional", testName = "container.offheap.OffHeapSizeTest")
public class OffHeapSizeTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .memory()
               .storage(StorageType.OFF_HEAP)
               .maxSize(ByteQuantity.Unit.MB.toBytes(10))
               .whenFull(EvictionStrategy.EXCEPTION)
            .transaction()
               .transactionMode(TransactionMode.TRANSACTIONAL);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   @DataProvider(name = "sizeMatchData")
   public Object[][] sizeMatchData() {
      return new Object[][] {
            { 10, 100, -1, -1, null },
            { 20, 313, -1, -1, null },
            { 10, 100, 4000, -1, null },
            { 20, 313, -1, 10000, null },
            { 10, 100, 4000, -1, new NumericVersion(1003)},
            { 20, 313, -1, 10000, new NumericVersion(81418) },
            { 10, 100, 4000, 738141, null},
            { 20, 313, 14141, 10000, new NumericVersion(8417) },
      };
   }

   @Test(dataProvider = "sizeMatchData")
   public void testSizeMatch(int keyLength, int valueLength, long maxIdle, long lifespan, EntryVersion version) {
      OffHeapMemoryAllocator allocator = TestingUtil.extractComponent(cache, OffHeapMemoryAllocator.class);
      long beginningSize = allocator.getAllocatedAmount();

      // We write directly to data container to avoid transformations
      ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
      byte[] keyBytes = new byte[keyLength];
      byte[] valueBytes = new byte[valueLength];
      threadLocalRandom.nextBytes(keyBytes);
      threadLocalRandom.nextBytes(valueBytes);

      WrappedBytes key = new WrappedByteArray(keyBytes);
      WrappedBytes value = new WrappedByteArray(valueBytes);

      EmbeddedMetadata.Builder metadataBuilder = new EmbeddedMetadata.Builder();

      if (maxIdle >= 0) {
         metadataBuilder.maxIdle(maxIdle);
      }
      if (lifespan >= 0) {
         metadataBuilder.lifespan(lifespan);
      }
      if (version != null) {
         metadataBuilder.version(version);
      }

      Metadata metadata = metadataBuilder.build();

      KeyValueMetadataSizeCalculator<WrappedBytes, WrappedBytes> calculator =
            TestingUtil.extractComponent(cache, KeyValueMetadataSizeCalculator.class);

      long estimateSize = calculator.calculateSize(key, value, metadata);

      cache.getAdvancedCache().getDataContainer().put(key, value, metadata);

      long endingSize = allocator.getAllocatedAmount();

      assertEquals(endingSize - beginningSize, estimateSize);
   }
}
