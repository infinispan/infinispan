package org.infinispan.configuration.cache;

import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.eviction.EvictionStrategy;

/**
 * Controls the data container for the cache.
 *
 * @author William Burns
 */
public class MemoryConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      Builder<MemoryConfiguration> {
   private final AttributeSet attributes;


   MemoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = MemoryConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Set the {@link StorageType} to determine how the data is stored in the data container.
    *
    * @param storageType the storage type of the underlying data
    * @return this configuration builder
    */
   public MemoryConfigurationBuilder storage(StorageType storageType) {
      attributes.attribute(MemoryConfiguration.STORAGE).set(storageType);
      return this;
   }

   public MemoryConfigurationBuilder maxSize(String size) {
      attributes.attribute(MemoryConfiguration.MAX_SIZE).set(size);
      return this;
   }

   public MemoryConfigurationBuilder maxSize(long l) {
      attributes.attribute(MemoryConfiguration.MAX_SIZE).set(Long.toString(l));
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
    *
    * @return the configured storage type
    */
   public StorageType storage() {
      return attributes.attribute(MemoryConfiguration.STORAGE).get();
   }

   /**
    * Sets the eviction strategy which can be:
    * <ul>
    *    <li>NONE - no eviction will take place</li>
    *    <li>MANUAL - no eviction will take place automatically, but user is assumed to manually call evict</li>
    *    <li>REMOVE - eviction will remove entries to make room for new entries to be inserted</li>
    *    <li>EXCEPTION - eviction will not take place, but instead an exception will be thrown to ensure container doesn't grow too large</li>
    * </ul>
    * <p>
    * The eviction strategy NONE and MANUAL are essentially the same except that MANUAL does not warn the user
    * when passivation is enabled.
    *
    * @param strategy the strategy to set
    * @return this
    */
   public MemoryConfigurationBuilder whenFull(EvictionStrategy strategy) {
      attributes.attribute(MemoryConfiguration.WHEN_FULL).set(strategy);
      return this;
   }

   public EvictionStrategy whenFull() {
      return attributes.attribute(MemoryConfiguration.WHEN_FULL).get();
   }

   boolean isSizeBounded() {
      return maxSize() != null;
   }

   boolean isCountBounded() {
      return maxCount() > 0;
   }

   private void checkBinaryRequirement() {
      if (!storage().canStoreReferences()) {
         if (getBuilder().clustering().hash().groups().isEnabled()) {
            throw CONFIG.groupingOnlyCompatibleWithObjectStorage(encoding().key().mediaType(), encoding().value().mediaType());
         }
      }

      boolean storageBinary = encoding().isStorageBinary() || !storage().canStoreReferences();
      if (isSizeBounded() && !storageBinary) {
         throw CONFIG.offHeapMemoryEvictionNotSupportedWithObject();
      }
   }

   @Override
   public void validate() {
      if (isSizeBounded() && isCountBounded()) {
         throw CONFIG.cannotProvideBothSizeAndCount();
      }
      EvictionStrategy strategy = whenFull();
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
      return new MemoryConfiguration(attributes.protect());
   }

   @Override
   public MemoryConfigurationBuilder read(MemoryConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "MemoryConfigurationBuilder{attributes=" + attributes + '}';
   }
}
