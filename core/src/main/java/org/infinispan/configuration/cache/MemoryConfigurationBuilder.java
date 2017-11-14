package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.MemoryConfiguration.ADDRESS_COUNT;
import static org.infinispan.configuration.cache.MemoryConfiguration.EVICTION_TYPE;
import static org.infinispan.configuration.cache.MemoryConfiguration.SIZE;
import static org.infinispan.configuration.cache.MemoryConfiguration.STORAGE_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.offheap.OffHeapDataContainer;
import org.infinispan.eviction.EvictionType;
import org.infinispan.util.logging.Log;

/**
 * Controls the data container for the cache.
 *
 * @author William Burns
 */
public class MemoryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<MemoryConfiguration> {

   private static final Log log = LogFactory.getLog(MemoryConfigurationBuilder.class, Log.class);

   private AttributeSet attributes;

   MemoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = MemoryConfiguration.attributeDefinitionSet();
   }

   /**
    * Set the {@link StorageType} to determine how the data is stored in the data container.
    * @param storageType the storage type of the underlying data
    * @return this configuration builder
    */
   public MemoryConfigurationBuilder storageType(StorageType storageType) {
      attributes.attribute(STORAGE_TYPE).set(storageType);
      return this;
   }

   /**
    * Defines the maximum size before eviction occurs. See {@link #evictionType(EvictionType)}
    * for more details on the size is interpreted.
    *
    * @param size
    */
   public MemoryConfigurationBuilder size(long size) {
      attributes.attribute(SIZE).set(size);
      return this;
   }

   /**
    * Sets the eviction type which can either be
    * <ul>
    * <li>COUNT - entries will be evicted when the number of entries exceeds the {@link #size(long)}</li>
    * <li>MEMORY - entries will be evicted when the approximate combined size of all values exceeds the {@link #size(long)}</li>
    * </ul>
    *
    * Cache size is guaranteed not to exceed upper
    * limit specified by max entries. However, due to the nature of eviction it is unlikely to ever
    * be exactly maximum number of entries specified here.
    *
    * @param type
    */
   public MemoryConfigurationBuilder evictionType(EvictionType type) {
      attributes.attribute(EVICTION_TYPE).set(type);
      return this;
   }

   /**
    * Configuration setting when using off-heap that defines how many address pointers there are.
    * This number will be rounded up to the next power of two.  This helps performance in that the
    * more address pointers there are the less collisions there will be which improve performance of
    * both read and write operations.
    * @param addressCount
    * @return this
    */
   public MemoryConfigurationBuilder addressCount(int addressCount) {
      attributes.attribute(ADDRESS_COUNT).set(addressCount);
      return this;
   }

   @Override
   public void validate() {
      StorageType type = attributes.attribute(STORAGE_TYPE).get();
      if (type != StorageType.OBJECT && getBuilder().compatibility().isEnabled()) {
         throw log.compatibilityModeOnlyCompatibleWithObjectStorage(type);
      }
      long size = attributes.attribute(SIZE).get();
      if (size > 0) {
         EvictionType evictionType = attributes.attribute(EVICTION_TYPE).get();
         if (evictionType == EvictionType.MEMORY) {
            switch (type) {
               case OBJECT:
                  throw log.offHeapMemoryEvictionNotSupportedWithObject();
               case OFF_HEAP:
                  int addressCount = attributes.attribute(ADDRESS_COUNT).get();
                  // Note this is cast to long as we have to multiply by 8 below which could overflow
                  long actualAddressCount = OffHeapDataContainer.getActualAddressCount(addressCount);
                  actualAddressCount *= 8;
                  if (size < actualAddressCount) {
                     throw log.offHeapMemoryEvictionSizeNotLargeEnoughForAddresses(size, actualAddressCount, addressCount);
                  }
            }
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public MemoryConfiguration create() {
      return new MemoryConfiguration(attributes.protect());
   }

   @Override
   public MemoryConfigurationBuilder read(MemoryConfiguration template) {
      attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "DataContainerConfigurationBuilder [attributes=" + attributes + "]";
   }
}
