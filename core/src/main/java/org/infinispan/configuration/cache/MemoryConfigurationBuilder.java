package org.infinispan.configuration.cache;

import static org.infinispan.util.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeListener;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the data container for the cache.
 *
 * @author William Burns
 */
public class MemoryConfigurationBuilder extends AbstractConfigurationChildBuilder implements
                                                                                  Builder<MemoryConfiguration> {
   private MemoryStorageConfigurationBuilder memoryStorageConfigurationBuilder;
   private final AttributeSet attributes;
   private final List<String> legacyAttributesUsed = new ArrayList<>();
   private boolean newAttributesUsed = false;
   private boolean isInListener = false;

   MemoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.memoryStorageConfigurationBuilder = new MemoryStorageConfigurationBuilder(builder);
      this.attributes = MemoryConfiguration.attributeDefinitionSet();

      // Keep new and legacy attributes in sync
      // Only attribute listeners are invoked when parsing JSON
      attributes.attribute(MemoryConfiguration.STORAGE)
                .addListener(nonReentrantListener((attribute, oldValue) -> {
                   memoryStorageAttribute(MemoryStorageConfiguration.STORAGE_TYPE).set(attribute.get());
                }));
      attributes.attribute(MemoryConfiguration.WHEN_FULL)
                .addListener(nonReentrantListener((attribute, oldValue) -> {
                   memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_STRATEGY).set(attribute.get());
                }));
      attributes.attribute(MemoryConfiguration.MAX_COUNT)
                .addListener(nonReentrantListener((attribute, oldValue) -> {
                   updateMaxCount(attribute.get());
                }));
      attributes.attribute(MemoryConfiguration.MAX_SIZE)
                .addListener(nonReentrantListener((attribute, oldValue) -> {
                   updateMaxSize(attribute.get());
                }));

      memoryStorageAttribute(MemoryStorageConfiguration.STORAGE_TYPE)
            .addListener(nonReentrantListener((attribute, oldValue) -> {
               attributes.attribute(MemoryConfiguration.STORAGE).set(attribute.get());
            }));
      memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_STRATEGY)
            .addListener(nonReentrantListener((attribute, oldValue) -> {
               attributes.attribute(MemoryConfiguration.WHEN_FULL).set(attribute.get());
            }));
      memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_TYPE)
            .addListener(nonReentrantListener((attribute, oldValue) -> {
               long size = memoryStorageAttribute(MemoryStorageConfiguration.SIZE).get();
               if (size == -1)
                  return;

               updateLegacySize(attribute.get(), size);
            }));
      memoryStorageAttribute(MemoryStorageConfiguration.SIZE)
            .addListener(nonReentrantListener((attribute, oldValue) -> {
               EvictionType evictionType = memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_TYPE).get();
               updateLegacySize(evictionType, attribute.get());
            }));
   }

   private <T> AttributeListener<T> nonReentrantListener(AttributeListener<T> listener) {
      return ((attribute, oldValue) -> {
         if (isInListener)
            return;
         isInListener = true;
         try {
            listener.attributeChanged(attribute, oldValue);
         } finally {
            isInListener = false;
         }
      });
   }

   /**
    * Set the {@link StorageType} to determine how the data is stored in the data container.
    * @param storageType the storage type of the underlying data
    * @return this configuration builder
    * @deprecated Since 11.0, use {@link #storage(StorageType)} instead.
    */
   @Deprecated
   public MemoryConfigurationBuilder storageType(StorageType storageType) {
      return storage(storageType);
   }

   public MemoryConfigurationBuilder storage(StorageType storageType) {
      attributes.attribute(MemoryConfiguration.STORAGE).set(storageType);
      return this;
   }

   public MemoryConfigurationBuilder maxSize(String size) {
      newAttributesUsed = true;
      attributes.attribute(MemoryConfiguration.MAX_SIZE).set(size);
      return this;
   }

   public String maxSize() {
      return attributes.attribute(MemoryConfiguration.MAX_SIZE).get();
   }

   public MemoryConfigurationBuilder maxCount(long count) {
      newAttributesUsed = true;
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
      return storage();
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
      legacyAttributesUsed.add(MemoryStorageConfiguration.SIZE.name());
      memoryStorageAttribute(MemoryStorageConfiguration.SIZE).set(size);
      return this;
   }

   /**
    * The configured eviction size, please see {@link MemoryConfigurationBuilder#size(long)}.
    * @return the configured evicted size
    * @deprecated Since 11.0, use either {@link #maxSize()} or {@link #maxCount()}.
    */
   @Deprecated
   public long size() {
      return memoryStorageAttribute(MemoryStorageConfiguration.SIZE).get();
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
      legacyAttributesUsed.add(MemoryStorageConfiguration.EVICTION_TYPE.name());
      memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_TYPE).set(type);
      return this;
   }

   private <T> Attribute<T> memoryStorageAttribute(AttributeDefinition<T> attributeDefinition) {
      return memoryStorageConfigurationBuilder.attributes.attribute(attributeDefinition);
   }

   /**
    * The configured eviction type, please see {@link MemoryConfigurationBuilder#evictionType(EvictionType)}.
    *
    * @return the configured eviction type
    * @deprecated since 11.0, @see {@link #evictionType(EvictionType)}
    */
   @Deprecated
   public EvictionType evictionType() {
      return memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_TYPE).get();
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
      return whenFull(strategy);
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
      return whenFull();
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
      if (!storageType().canStoreReferences()) {
         if (getBuilder().clustering().hash().groups().isEnabled()) {
            throw CONFIG.groupingOnlyCompatibleWithObjectStorage(keyType, valueType);
         }
      }

      boolean storageBinary = encoding().isStorageBinary() || !storageType().canStoreReferences();
      if (isSizeBounded() && !storageBinary) {
         throw CONFIG.offHeapMemoryEvictionNotSupportedWithObject();
      }
   }

   @Override
   public void validate() {
      if (newAttributesUsed && !legacyAttributesUsed.isEmpty()) {
         // Prevent mixing of old and new attributes
         throw CONFIG.cannotUseDeprecatedAndReplacement(legacyAttributesUsed.toString());
      }
      if (isSizeBounded() && isCountBounded()) {
         throw CONFIG.cannotProvideBothSizeAndCount();
      }

      EvictionStrategy strategy = evictionStrategy();
      if (strategy.isEnabled()) {
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
      } else {
         if (getBuilder().persistence().passivation() && strategy != EvictionStrategy.MANUAL &&
             !getBuilder().template()) {
            // maxSize of maxCount would automatically set evictionStrategy(REMOVAL)
            if (!isSizeBounded() && !isCountBounded()) {
               CONFIG.passivationWithoutEviction();
            }
         }
      }
      checkBinaryRequirement();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public MemoryConfiguration create() {
      Attribute<EvictionStrategy> whenFull = attributes.attribute(MemoryConfiguration.WHEN_FULL);
      if (!whenFull.get().isEnabled()) {
         if (isSizeBounded() || isCountBounded()) {
            whenFull.setImplied(EvictionStrategy.REMOVE);
            if (isCountBounded()) {
               CONFIG.debugf("Max entries configured (%d) without eviction strategy. Eviction strategy overridden to %s", maxCount(), whenFull.get());
            } else {
               CONFIG.debugf("Max size configured (%s) without eviction strategy. Eviction strategy overridden to %s", maxSize(), whenFull.get());
            }
         }
      }
      return new MemoryConfiguration(attributes.protect(), memoryStorageConfigurationBuilder.create());
   }

   @Override
   public MemoryConfigurationBuilder read(MemoryConfiguration template) {
      attributes.read(template.attributes());

      // Propagate any changes to the MemoryStorageConfiguration attributes,
      // because reading an attribute does not invoke the listener
      // There's no need to do the reverse, since the new and legacy attributes
      // are in sync at build time
      if (attributes.attribute(MemoryConfiguration.STORAGE).isModified()) {
         memoryStorageAttribute(MemoryStorageConfiguration.STORAGE_TYPE).set(storage());
      }
      if (attributes.attribute(MemoryConfiguration.WHEN_FULL).isModified()) {
         memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_STRATEGY).set(whenFull());
      }
      if (attributes.attribute(MemoryConfiguration.MAX_COUNT).isModified()) {
         updateMaxCount(maxCount());
      }
      if (attributes.attribute(MemoryConfiguration.MAX_SIZE).isModified()) {
         updateMaxSize(maxSize());
      }
      return this;
   }

   private void updateMaxSize(String maxSize) {
      Attribute<EvictionType> evictionTypeAttribute =
            memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_TYPE);
      if (maxSize != null && evictionTypeAttribute.get() != EvictionType.MEMORY) {
         evictionTypeAttribute.set(EvictionType.MEMORY);
      }
      if (maxSize != null || maxCount() == -1L) {
         // Either the new maxSize is valid or both maxSize and maxCount are -1
         long maxSizeBytes = MemoryConfiguration.maxSizeToBytes(maxSize);
         memoryStorageAttribute(MemoryStorageConfiguration.SIZE).set(maxSizeBytes);
      }
   }

   private void updateMaxCount(long maxCount) {
      Attribute<EvictionType> evictionTypeAttribute =
            memoryStorageAttribute(MemoryStorageConfiguration.EVICTION_TYPE);
      if (maxCount != -1 && evictionTypeAttribute.get() != EvictionType.COUNT) {
         evictionTypeAttribute.set(EvictionType.COUNT);
      }
      if (maxCount != -1L || maxSize() == null) {
         // Either the new maxCount is valid or both maxSize and maxCount are -1
         memoryStorageAttribute(MemoryStorageConfiguration.SIZE).set(maxCount);
      }
   }

   private void updateLegacySize(EvictionType type, Long size) {
      switch (type) {
         case COUNT:
            if (attributes.attribute(MemoryConfiguration.MAX_SIZE).get() != null) {
               attributes.attribute(MemoryConfiguration.MAX_SIZE).reset();
            }
            attributes.attribute(MemoryConfiguration.MAX_COUNT).set(size);
            break;
         case MEMORY:
            if (attributes.attribute(MemoryConfiguration.MAX_COUNT).get() != -1L) {
               attributes.attribute(MemoryConfiguration.MAX_COUNT).reset();
            }

            String maxSize = attributes.attribute(MemoryConfiguration.MAX_SIZE).get();
            long maxSizeBytes = MemoryConfiguration.maxSizeToBytes(maxSize);
            if (maxSizeBytes != size) {
               attributes.attribute(MemoryConfiguration.MAX_SIZE).set(String.valueOf(size));
            }
            break;
         default:
            throw new IllegalArgumentException();
      }
   }

   @Override
   public String toString() {
      return "MemoryConfigurationBuilder{" +
            "memoryStorageConfigurationBuilder=" + memoryStorageConfigurationBuilder +
            ", attributes=" + attributes +
            '}';
   }
}
