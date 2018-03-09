package org.infinispan.factories;

import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.KeyValueMetadataSizeCalculator;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.container.offheap.OffHeapEntryFactory;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;

/**
 * Factory for creating size calculator used to estimate size of objects
 * @author wburns
 * @since 9.0
 */
@DefaultFactoryFor(classes = KeyValueMetadataSizeCalculator.class)
public class SizeCalculatorFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      MemoryConfiguration memory = configuration.memory();
      if (memory.evictionStrategy().isEnabled() && memory.evictionType() == EvictionType.MEMORY) {
         StorageType type = memory.storageType();
         switch (type) {
            case BINARY:
               return componentType.cast(configuration.transaction().transactionMode().isTransactional() ?
                     CacheEntrySingleton.WITHOUT_INVOCATION_RECORDS : CacheEntrySingleton.WITH_INVOCATION_RECORDS);
            case OFF_HEAP:
               return componentType.cast(componentRegistry.getComponent(OffHeapEntryFactory.class));
            case OBJECT:
               /**
                * We can't have object based when eviction is memory based. The
                * {@link org.infinispan.configuration.cache.MemoryConfigurationBuilder#validate()} should handle
                * checking for this.
                */
            default:
               throw new UnsupportedOperationException();
         }
      } else {
         return componentType.cast((KeyValueMetadataSizeCalculator) (k, v, m) -> 1);
      }
   }

   static class CacheEntrySingleton {
      static final CacheEntrySizeCalculator WITH_INVOCATION_RECORDS = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
            new PrimitiveEntrySizeCalculator()), true);
      static final CacheEntrySizeCalculator WITHOUT_INVOCATION_RECORDS = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
            new PrimitiveEntrySizeCalculator()), false);
   }
}
