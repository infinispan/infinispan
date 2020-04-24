package org.infinispan.configuration.cache;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
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
   private final AttributeSet attributes;
   private final AttributeChangeTracker attributeTracker;

   MemoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.memoryStorageConfigurationBuilder = new MemoryStorageConfigurationBuilder(builder);
      this.elements = Collections.singletonList(memoryStorageConfigurationBuilder);
      this.attributes = MemoryConfiguration.attributeDefinitionSet();
      this.attributeTracker = new AttributeChangeTracker(attributes);
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return elements;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Set the {@link StorageType} to determine how the data is stored in the data container.
    * @param storageType the storage type of the underlying data
    * @return this configuration builder
    * @deprecated Since 11.0, use {@link #storage(StorageType)} instead.
    */
   @Deprecated
   public MemoryConfigurationBuilder storageType(StorageType storageType) {
      memoryStorageConfigurationBuilder.storageType(storageType);
      return this;
   }

   public MemoryConfigurationBuilder storage(StorageType storageType) {
      if (storageType != null) {
         switch (storageType) {
            case OBJECT:
            case HEAP:
               attributes.attribute(MemoryConfiguration.STORAGE).set(StorageType.HEAP);
               break;
            default:
               attributes.attribute(MemoryConfiguration.STORAGE).set(storageType);
         }
      }
      return this;
   }

   public MemoryConfigurationBuilder maxSize(String size) {
      attributes.attribute(MemoryConfiguration.MAX_SIZE).set(size);
      return this;
   }

   public String maxSize() {
      return attributes.attribute(MemoryConfiguration.MAX_SIZE).get();
   }

   public MemoryConfigurationBuilder maxCount(long count) {
      attributes.attribute(MemoryConfiguration.MAX_COUNT).set(count);
      return this;
   }

   public long maxCount() {
      return attributes.attribute(MemoryConfiguration.MAX_COUNT).get();
   }

   /**
    * The underlying storage type for this configuration
    * @return the configured storage type
    * @deprecated Since 11.0, use {@link #storage()} instead.
    */
   @Deprecated
   public StorageType storageType() {
      return memoryStorageConfigurationBuilder.storageType();
   }

   public StorageType storage() {
      return attributes.attribute(MemoryConfiguration.STORAGE).get();
   }

   /**
    * Defines the maximum size before eviction occurs. See {@link #evictionType(EvictionType)}
    * for more details on the size is interpreted.
    *
    * If {@link #evictionStrategy(EvictionStrategy)} has not been invoked, this will set the strategy to
    * {@link EvictionStrategy#REMOVE}.
    *
    * @param size the maximum size for the container
    * @deprecated Since 11.0, use {@link #maxSize(String)} to define the size in bytes or {@link #maxCount(long)}
    * to define the number of entries.
    */
   @Deprecated
   public MemoryConfigurationBuilder size(long size) {
      memoryStorageConfigurationBuilder.size(size);
      return this;
   }

   @Override
   public ElementDefinition<?> getElementDefinition() {
      return MemoryConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * The configured eviction size, please see {@link MemoryConfigurationBuilder#size(long)}.
    * @return the configured evicted size
    * @deprecated Since 11.0, use either {@link #maxSize()} or {@link #maxCount()}.
    */
   @Deprecated
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
    * @deprecated since 11.0, use {@link #maxCount(long)} or {@link #maxSize(String)} to define data container bounds
    * by size or by count.
    */
   @Deprecated
   public MemoryConfigurationBuilder evictionType(EvictionType type) {
      memoryStorageConfigurationBuilder.evictionType(type);
      return this;
   }

   /**
    * The configured eviction type, please see {@link MemoryConfigurationBuilder#evictionType(EvictionType)}.
    * @return the configured eviction type
    * @deprecated since 11.0, @see {@link #evictionType(EvictionType)}
    */
   @Deprecated
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
    * @deprecated Since 11.0, use {@link #whenFull(EvictionStrategy)} instead.
    */
   @Deprecated
   public MemoryConfigurationBuilder evictionStrategy(EvictionStrategy strategy) {
      memoryStorageConfigurationBuilder.evictionStrategy(strategy);
      return this;
   }

   public MemoryConfigurationBuilder whenFull(EvictionStrategy strategy) {
      attributes.attribute(MemoryConfiguration.WHEN_FULL).set(strategy);
      return this;
   }

   public EvictionStrategy whenFull() {
      return attributes.attribute(MemoryConfiguration.WHEN_FULL).get();
   }

   /**
    * The configured eviction strategy, please see {@link MemoryConfigurationBuilder#evictionStrategy(EvictionStrategy)}.
    * @return the configured eviction stategy
    * @deprecated Since 11.0, use {@link #whenFull()} instead.
    */
   @Deprecated
   public EvictionStrategy evictionStrategy() {
      return memoryStorageConfigurationBuilder.evictionStrategy();
   }

   boolean isSizeBounded() {
      return maxSize() != null;
   }

   boolean isCountBounded() {
      return maxCount() > 0;
   }

   private void checkBinaryRequirement() {
      String keyType = encoding().key().mediaType();
      String valueType = encoding().value().mediaType();
      if (storageType() != StorageType.HEAP && storageType() != StorageType.OBJECT) {
         if (getBuilder().clustering().hash().groups().isEnabled()) {
            throw CONFIG.groupingOnlyCompatibleWithObjectStorage(keyType, valueType);
         }
      }

      boolean storageBinary = encoding().isStorageBinary() || storageType() != StorageType.HEAP;
      if (isSizeBounded() && !storageBinary) {
         throw CONFIG.offHeapMemoryEvictionNotSupportedWithObject();
      }
   }

   @Override
   public void validate() {
      // State Changes cannot be done here because validation can be turned off in the ConfigurationBuilder
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public MemoryConfiguration create() {
      AttributeChangeTracker legacyAttributeTracker = memoryStorageConfigurationBuilder.getAttributeTracker();
      legacyAttributeTracker.stopChangeTracking();
      boolean hasLegacyChanges = legacyAttributeTracker.hasChanges();
      String changedLegacyAttributes = legacyAttributeTracker.getChangedAttributes();

      attributeTracker.stopChangeTracking();
      boolean hasChanges = attributeTracker.hasChanges();

      if (hasChanges && hasLegacyChanges) {
         // Prevent mixing of old and new attributes
         throw CONFIG.cannotUseDeprecatedAndReplacement(changedLegacyAttributes);
      }

      if (hasLegacyChanges) {
         // The builder used legacy/deprecated attributes
         CONFIG.warnUsingDeprecatedMemoryConfigs(changedLegacyAttributes);
      }

      if (hasLegacyChanges || memoryStorageConfigurationBuilder.isEnabled()) {
         memoryStorageConfigurationBuilder.enable();
         // Translate deprecated attributes to the new ones
         this.read(memoryStorageConfigurationBuilder);
      } else {
         // Only new attributes were changed, translate them to legacy attributes for backwards compatibility
         memoryStorageConfigurationBuilder.read(this);
      }

      if (isSizeBounded() && isCountBounded()) {
         throw CONFIG.cannotProvideBothSizeAndCount();
      }
      checkBinaryRequirement();

      EvictionStrategy strategy = evictionStrategy();
      if (!strategy.isEnabled()) {
         if (isSizeBounded() || isCountBounded()) {
            EvictionStrategy newStrategy = EvictionStrategy.REMOVE;
            whenFull(newStrategy);
            memoryStorageConfigurationBuilder.evictionStrategy(newStrategy);
            if (isCountBounded()) {
               CONFIG.debugf("Max entries configured (%d) without eviction strategy. Eviction strategy overridden to %s", maxCount(), newStrategy);
            } else {
               CONFIG.debugf("Max size configured (%s) without eviction strategy. Eviction strategy overridden to %s", maxSize(), newStrategy);
            }
         } else if (getBuilder().persistence().passivation() && strategy != EvictionStrategy.MANUAL &&
               !getBuilder().template()) {
            CONFIG.passivationWithoutEviction();
         }
      } else {
         if (!isCountBounded() && !isSizeBounded()) {
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
      attributeTracker.reset();
      return new MemoryConfiguration(attributes.protect(), memoryStorageConfigurationBuilder.create());
   }

   @Override
   public MemoryConfigurationBuilder read(MemoryConfiguration template) {
      MemoryStorageConfiguration legacyConfig = template.heapConfiguration();
      if (!legacyConfig.isEnabled()) {
         attributes.read(template.attributes());
      } else {
         memoryStorageConfigurationBuilder.read(legacyConfig);
      }
      return this;
   }

   private void read(MemoryStorageConfigurationBuilder memoryStorageConfigurationBuilder) {
      long legacySize = memoryStorageConfigurationBuilder.size();
      if (memoryStorageConfigurationBuilder.evictionType() == EvictionType.MEMORY) {
         maxSize(String.valueOf(legacySize));
      } else {
         maxCount(legacySize);
      }
      whenFull(memoryStorageConfigurationBuilder.evictionStrategy());
      StorageType storageType = memoryStorageConfigurationBuilder.storageType();
      if (storageType == StorageType.OBJECT) {
         storage(StorageType.HEAP);
      } else {
         storage(storageType);
      }
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
            ", attributes=" + attributes +
            '}';
   }
}
