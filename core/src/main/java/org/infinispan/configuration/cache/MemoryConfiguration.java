package org.infinispan.configuration.cache;

import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.ByteQuantity;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;

/**
 * Controls the memory storage configuration for the cache.
 *
 * @author William Burns
 */
public class MemoryConfiguration extends ConfigurationElement<MemoryConfiguration> {

   public static final AttributeDefinition<StorageType> STORAGE = AttributeDefinition.builder(Attribute.STORAGE, StorageType.HEAP).immutable().build();
   public static final AttributeDefinition<String> MAX_SIZE = AttributeDefinition.builder(Attribute.MAX_SIZE, null, String.class).build();
   public static final AttributeDefinition<Long> MAX_COUNT = AttributeDefinition.builder(Attribute.MAX_COUNT, -1L).build();
   public static final AttributeDefinition<EvictionStrategy> WHEN_FULL = AttributeDefinition.builder(Attribute.WHEN_FULL, EvictionStrategy.NONE).immutable().build();

   private final MemoryStorageConfiguration memoryStorageConfiguration;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(MemoryConfiguration.class, STORAGE, MAX_SIZE, MAX_COUNT, WHEN_FULL);
   }

   MemoryConfiguration(AttributeSet attributes, MemoryStorageConfiguration memoryStorageConfiguration) {
      super(Element.MEMORY, attributes);
      this.memoryStorageConfiguration = memoryStorageConfiguration;
      // Add a listener to keep new attributes in sync with legacy size without complicating MemoryStorageConfiguration
      // Size is the only legacy attribute that can modify at runtime
      // Unlike the builder, updates to the new attributes are handled directly in the setters
      memoryStorageConfiguration.attributes().attribute(MemoryStorageConfiguration.SIZE)
                                .addListener((a, oldValue) -> updateSize(a.get()));
   }

   /**
    * @return true if the storage is off-heap
    */
   public boolean isOffHeap() {
      return attributes.attribute(STORAGE).get() == StorageType.OFF_HEAP;
   }

   /**
    * @return The max size in bytes or -1 if not configured.
    */
   public long maxSizeBytes() {
      return maxSizeToBytes(maxSize());
   }

   public String maxSize() {
      return attributes.attribute(MAX_SIZE).get();
   }

   public void maxSize(String maxSize) {
      if (!isSizeBounded()) throw CONFIG.cannotChangeMaxSize();

      attributes.attribute(MAX_SIZE).set(maxSize);
      memoryStorageConfiguration.attributes().attribute(MemoryStorageConfiguration.SIZE).set(maxSizeToBytes(maxSize));
   }

   /**
    * @return the max number of entries in memory or -1 if not configured.
    */
   public long maxCount() {
      return attributes.attribute(MAX_COUNT).get();
   }

   public void maxCount(long maxCount) {
      if (!isCountBounded()) throw CONFIG.cannotChangeMaxCount();

      attributes.attribute(MAX_COUNT).set(maxCount);
      memoryStorageConfiguration.attributes().attribute(MemoryStorageConfiguration.SIZE).set(maxCount);
   }

   /**
    * Storage type to use for the data container
    * @deprecated Use {@link #storage()} instead.
    */
   @Deprecated
   public StorageType storageType() {
      return storage();
   }

   /**
    * @return The memory {@link StorageType}.
    */
   public StorageType storage() {
      return attributes.attribute(STORAGE).get();
   }

   /**
    * Size of the eviction, -1 if disabled
    * @deprecated Since 11.0, use {@link #maxCount()} or {@link #maxSize()} to obtain
    * either the maximum number of entries or the maximum size of the data container.
    */
   @Deprecated
   public long size() {
      return memoryStorageConfiguration.size();
   }

   /**
    * @deprecated Since 11.0, use {@link MemoryConfiguration#maxCount(long)} or
    * {@link MemoryConfiguration#maxSize(String)} to dynamically configure the maximum number
    * of entries or the maximum size of the data container.
    */
   @Deprecated
   public void size(long newSize) {
      memoryStorageConfiguration.size(newSize);
   }

   private void updateSize(long newSize) {
      if (isCountBounded()) {
         attributes.attribute(MAX_COUNT).set(newSize);
      } else {
         attributes.attribute(MAX_SIZE).set(String.valueOf(newSize));
      }
   }

   /**
    * The configured eviction type
    *
    * @deprecated Since 11.0, use {@link #maxCount()} or {@link #maxSize()} to obtain either the maximum number of
    *       entries or the maximum size of the data container.
    */
   @Deprecated
   public EvictionType evictionType() {
      return memoryStorageConfiguration.evictionType();
   }

   /**
    * The configured eviction strategy
    * @deprecated Since 11.0, use {@link #whenFull()}
    */
   @Deprecated
   public EvictionStrategy evictionStrategy() {
      return memoryStorageConfiguration.evictionStrategy();
   }

   /**
    * @return The configured {@link EvictionStrategy}.
    */
   public EvictionStrategy whenFull() {
      return attributes.attribute(WHEN_FULL).get();
   }

   /**
    * Returns whether remove eviction is in use
    */
   public boolean isEvictionEnabled() {
      return (isSizeBounded() || isCountBounded()) && whenFull().isRemovalBased();
   }

   private boolean isSizeBounded() {
      return maxSize() != null;
   }

   private boolean isCountBounded() {
      return maxCount() > 0;
   }

   /**
    * @deprecated Since 11.0, use {@link #evictionStrategy()}, {@link #maxSize()},
    *       {@link #maxCount()}, {@link #isOffHeap()} instead
    */
   @Deprecated
   public MemoryStorageConfiguration heapConfiguration() {
      return memoryStorageConfiguration;
   }

   static long maxSizeToBytes(String maxSizeStr) {
      return maxSizeStr != null ? ByteQuantity.parse(maxSizeStr) : -1;
   }
}
