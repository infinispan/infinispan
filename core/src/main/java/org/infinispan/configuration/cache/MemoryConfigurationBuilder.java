package org.infinispan.configuration.cache;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the data container for the cache.
 *
 * @author William Burns
 */
public class MemoryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<MemoryConfiguration>, ConfigurationBuilderInfo {
   private MemoryStorageConfigurationBuilder memoryStorageConfigurationBuilder;
   private final List<ConfigurationBuilderInfo> elements;

   MemoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.memoryStorageConfigurationBuilder = new MemoryStorageConfigurationBuilder(builder);
      this.elements = Collections.singletonList(memoryStorageConfigurationBuilder);
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return elements;
   }

   /**
    * Set the {@link StorageType} to determine how the data is stored in the data container.
    * @param storageType the storage type of the underlying data
    * @return this configuration builder
    */
   public MemoryConfigurationBuilder storageType(StorageType storageType) {
      memoryStorageConfigurationBuilder.storageType(storageType);
      return this;
   }

   /**
    * The underlying storage type for this configuration
    * @return the configured storage type
    */
   public StorageType storageType() {
      return memoryStorageConfigurationBuilder.storageType();
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
      memoryStorageConfigurationBuilder.size(size);
      return this;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return MemoryConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * The configured eviction size, please see {@link MemoryConfigurationBuilder#size(long)}.
    * @return the configured evicted size
    */
   public long size() {
      return memoryStorageConfigurationBuilder.size();
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
      memoryStorageConfigurationBuilder.evictionType(type);
      return this;
   }

   /**
    * The configured eviction type, please see {@link MemoryConfigurationBuilder#evictionType(EvictionType)}.
    * @return the configured eviction type
    */
   public EvictionType evictionType() {
      return memoryStorageConfigurationBuilder.evictionType();
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
      memoryStorageConfigurationBuilder.evictionStrategy(strategy);
      return this;
   }

   /**
    * The configured eviction strategy, please see {@link MemoryConfigurationBuilder#evictionStrategy(EvictionStrategy)}.
    * @return the configured eviction stategy
    */
   public EvictionStrategy evictionStrategy() {
      return memoryStorageConfigurationBuilder.evictionStrategy();
   }

   /**
    * Configuration setting when using off-heap that defines how many address pointers there are.
    * This number will be rounded up to the next power of two.  This helps performance in that the
    * more address pointers there are the less collisions there will be which improve performance of
    * both read and write operations. This is only used when OFF_HEAP storage type is configured
    * {@link MemoryConfigurationBuilder#storageType(StorageType)}.
    * @param addressCount
    * @return this
    * @deprecated since 10.0
    */
   @Deprecated
   public MemoryConfigurationBuilder addressCount(int addressCount) {
      memoryStorageConfigurationBuilder.addressCount(addressCount);
      return this;
   }

   /**
    * How many address pointers are configured for the off heap storage. See
    * {@link MemoryConfigurationBuilder#addressCount(int)} for more information.
    * @return the configured amount of address pointers
    * @deprecated since 10.0
    */
   @Deprecated
   public int addressCount() {
      return memoryStorageConfigurationBuilder.addressCount();
   }

   @Override
   public void validate() {
      StorageType type = memoryStorageConfigurationBuilder.storageType();
      if (type != StorageType.OBJECT) {
         if (getBuilder().clustering().hash().groups().isEnabled()) {
            throw CONFIG.groupingOnlyCompatibleWithObjectStorage(type);
         }
      }

      long size = memoryStorageConfigurationBuilder.size();
      EvictionType evictionType = memoryStorageConfigurationBuilder.evictionType();
      if (evictionType == EvictionType.MEMORY) {
         switch (type) {
            case OBJECT:
               throw CONFIG.offHeapMemoryEvictionNotSupportedWithObject();
         }
      }

      EvictionStrategy strategy = memoryStorageConfigurationBuilder.evictionStrategy();
      if (!strategy.isEnabled()) {
         if (size > 0) {
            EvictionStrategy newStrategy = EvictionStrategy.REMOVE;
            evictionStrategy(newStrategy);
            CONFIG.debugf("Max entries configured (%d) without eviction strategy. Eviction strategy overridden to %s", size, newStrategy);
         } else if (getBuilder().persistence().passivation() && strategy != EvictionStrategy.MANUAL &&
               !getBuilder().template()) {
            CONFIG.passivationWithoutEviction();
         }
      } else {
         if (size <= 0) {
            throw CONFIG.invalidEvictionSize();
         }
         if (strategy.isExceptionBased()) {
            TransactionConfigurationBuilder transactionConfiguration = getBuilder().transaction();
            org.infinispan.transaction.TransactionMode transactionMode = transactionConfiguration.transactionMode();
            if (transactionMode == null || !transactionMode.isTransactional() ||
                  transactionConfiguration.useSynchronization() ||
                  transactionConfiguration.use1PcForAutoCommitTransactions()) {
               throw CONFIG.exceptionBasedEvictionOnlySupportedInTransactionalCaches();
            }
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public MemoryConfiguration create() {
      return new MemoryConfiguration(memoryStorageConfigurationBuilder.create());
   }

   @Override
   public MemoryConfigurationBuilder read(MemoryConfiguration template) {
      memoryStorageConfigurationBuilder.read(template.heapConfiguration());
      return this;
   }

   @Override
   public ConfigurationBuilderInfo getBuilderInfo(String name, String qualifier) {
      switch (name) {
         case "off-heap":
            memoryStorageConfigurationBuilder.storageType(StorageType.OFF_HEAP);
            break;
         case "binary":
            memoryStorageConfigurationBuilder.storageType(StorageType.BINARY);
            break;
         case "object":
            memoryStorageConfigurationBuilder.storageType(StorageType.OBJECT);
            break;
      }
      return memoryStorageConfigurationBuilder;
   }

   @Override
   public String toString() {
      return "MemoryConfigurationBuilder{" +
            "memoryStorageConfigurationBuilder=" + memoryStorageConfigurationBuilder +
            '}';
   }
}
