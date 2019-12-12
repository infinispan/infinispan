package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.MemoryConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionType;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteOffHeapNonIndexedQueryDslConditionsTest")
public class RemoteQueryAlternativeStorageNonIndexedDslConditionsTest extends RemoteNonIndexedQueryDslConditionsTest {

   private StorageType storageType;
   private EvictionType evictionType;

   @Override
   protected String parameters() {
      return "[" + storageType + ", " + evictionType + "]";
   }

   public Object[] factory() {
      return new Object[] {
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.OFF_HEAP),
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.BINARY),
            // OBJECT is excluded as the base class already tests that
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.OFF_HEAP).evictionType(EvictionType.COUNT),
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.BINARY).evictionType(EvictionType.COUNT),
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.OBJECT).evictionType(EvictionType.COUNT),
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.OFF_HEAP).evictionType(EvictionType.MEMORY),
            new RemoteQueryAlternativeStorageNonIndexedDslConditionsTest().storageType(StorageType.BINARY).evictionType(EvictionType.MEMORY),
      };
   }

   RemoteQueryAlternativeStorageNonIndexedDslConditionsTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   RemoteQueryAlternativeStorageNonIndexedDslConditionsTest evictionType(EvictionType evictionType) {
      this.evictionType = evictionType;
      return this;
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = super.getConfigurationBuilder();
      MemoryConfigurationBuilder memoryConfigurationBuilder = builder.memory();
      memoryConfigurationBuilder
            .storageType(storageType);
      if (evictionType != null) {
         memoryConfigurationBuilder
               .evictionType(evictionType)
               // Make sure size is enough for memory to not evict anything
               .size(10_000_000);
      }
      return builder;
   }
}
