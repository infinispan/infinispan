package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.MemoryConfiguration.ADDRESS_COUNT;
import static org.infinispan.configuration.cache.MemoryConfiguration.EVICTION_STRATEGY;
import static org.infinispan.configuration.cache.MemoryConfiguration.EVICTION_TYPE;
import static org.infinispan.configuration.cache.MemoryConfiguration.SIZE;
import static org.infinispan.configuration.cache.MemoryConfiguration.STORAGE_TYPE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.offheap.OffHeapDataContainer;
import org.infinispan.container.offheap.UnpooledOffHeapMemoryAllocator;
import org.infinispan.eviction.EvictionStrategy;
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
    * The underlying storage type for this configuration
    * @return the configured storage type
    */
   public StorageType storageType() {
      return attributes.attribute(STORAGE_TYPE).get();
   }

   /**
    * Defines the maximum size before eviction occurs. See {@link #evictionType(EvictionType)}
    * for more details on the size is interpreted.
    *
    * If {@link #evictionStrategy(EvictionStrategy)} has not been invoked, this will set the strategy to
    * {@link EvictionStrategy#REMOVE}.
    *
    * @param size the maximum size for the container
    */
   public MemoryConfigurationBuilder size(long size) {
      attributes.attribute(SIZE).set(size);
      return this;
   }

   /**
    * The configured eviction size, please see {@link MemoryConfigurationBuilder#size(long)}.
    * @return the configured evicted size
    */
   public long size() {
      return attributes.attribute(SIZE).get();
   }

   /**
    * Sets the eviction type which can either be
    * <ul>
    * <li>COUNT - entries will be evicted when the number of entries exceeds the {@link #size(long)}</li>
    * <li>MEMORY - entries will be evicted when the approximate combined size of all values exceeds the {@link #size(long)}</li>
    * </ul>
    *
    * Cache size is guaranteed not to exceed upper
    * limit specified by size.
    *
    * @param type
    */
   public MemoryConfigurationBuilder evictionType(EvictionType type) {
      attributes.attribute(EVICTION_TYPE).set(type);
      return this;
   }

   /**
    * The configured eviction type, please see {@link MemoryConfigurationBuilder#evictionType(EvictionType)}.
    * @return the configured eviction type
    */
   public EvictionType evictionType() {
      return attributes.attribute(EVICTION_TYPE).get();
   }

   /**
    * Sets the eviction strategy which can be:
    * <ul>
    *    <li>NONE - no eviction will take place</li>
    *    <li>MANUAL - no eviction will take place automatically, but user is assumed to manually call evict</li>
    *    <li>REMOVE - eviction will remove entries to make room for new entries to be inserted</li>
    *    <li>EXCEPTION - eviction will not take place, but instead an exception will be thrown to ensure container doesn't grow too large</li>
    * </ul>
    *
    * The eviction strategy NONE and MANUAL are essentially the same except that MANUAL does not warn the user
    * when passivation is enabled.
    * @param strategy the strategy to set
    * @return this
    */
   public MemoryConfigurationBuilder evictionStrategy(EvictionStrategy strategy) {
      attributes.attribute(EVICTION_STRATEGY).set(strategy);
      return this;
   }

   /**
    * The configured eviction strategy, please see {@link MemoryConfigurationBuilder#evictionStrategy(EvictionStrategy)}.
    * @return the configured eviction stategy
    */
   public EvictionStrategy evictionStrategy() {
      return attributes.attribute(EVICTION_STRATEGY).get();
   }

   /**
    * Configuration setting when using off-heap that defines how many address pointers there are.
    * This number will be rounded up to the next power of two.  This helps performance in that the
    * more address pointers there are the less collisions there will be which improve performance of
    * both read and write operations. This is only used when OFF_HEAP storage type is configured
    * {@link MemoryConfigurationBuilder#storageType(StorageType)}.
    * @param addressCount
    * @return this
    */
   public MemoryConfigurationBuilder addressCount(int addressCount) {
      attributes.attribute(ADDRESS_COUNT).set(addressCount);
      return this;
   }

   /**
    * How many address pointers are configured for the off heap storage. See
    * {@link MemoryConfigurationBuilder#addressCount(int)} for more information.
    * @return the configured amount of address pointers
    */
   public int addressCount() {
      return attributes.attribute(ADDRESS_COUNT).get();
   }

   @Override
   public void validate() {
      StorageType type = attributes.attribute(STORAGE_TYPE).get();
      if (type != StorageType.OBJECT && getBuilder().compatibility().isEnabled()) {
         throw log.compatibilityModeOnlyCompatibleWithObjectStorage(type);
      }

      long size = attributes.attribute(SIZE).get();
      EvictionType evictionType = attributes.attribute(EVICTION_TYPE).get();
      if (evictionType == EvictionType.MEMORY) {
         switch (type) {
            case OBJECT:
               throw log.offHeapMemoryEvictionNotSupportedWithObject();
            case OFF_HEAP:
               int addressCount = attributes.attribute(ADDRESS_COUNT).get();
               // Note this is cast to long as we have to multiply by 8 below which could overflow
               long actualAddressCount = OffHeapDataContainer.getActualAddressCount(addressCount << 3);
               actualAddressCount = UnpooledOffHeapMemoryAllocator.estimateSizeOverhead(actualAddressCount);
               if (size < actualAddressCount) {
                  throw log.offHeapMemoryEvictionSizeNotLargeEnoughForAddresses(size, actualAddressCount, addressCount);
               }
         }
      }

      EvictionStrategy strategy = attributes.attribute(EVICTION_STRATEGY).get();
      if (!strategy.isEnabled()) {
         if (size > 0) {
            evictionStrategy(EvictionStrategy.REMOVE);
            log.debugf("Max entries configured (%d) without eviction strategy. Eviction strategy overridden to %s", size, strategy);
         } else if (getBuilder().persistence().passivation() && strategy != EvictionStrategy.MANUAL &&
               !getBuilder().template()) {
            log.passivationWithoutEviction();
         }
      } else {
         if (size <= 0) {
            throw log.invalidEvictionSize();
         }
         if (strategy.isExceptionBased()) {
            TransactionConfigurationBuilder transactionConfiguration = getBuilder().transaction();
            org.infinispan.transaction.TransactionMode transactionMode = transactionConfiguration.transactionMode();
            if (transactionMode == null || !transactionMode.isTransactional() ||
                  transactionConfiguration.useSynchronization() ||
                  transactionConfiguration.use1PcForAutoCommitTransactions()) {
               throw log.exceptionBasedEvictionOnlySupportedInTransactionalCaches();
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
