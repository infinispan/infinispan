package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.MemoryStorageConfiguration.ADDRESS_COUNT;
import static org.infinispan.configuration.cache.MemoryStorageConfiguration.EVICTION_STRATEGY;
import static org.infinispan.configuration.cache.MemoryStorageConfiguration.EVICTION_TYPE;
import static org.infinispan.configuration.cache.MemoryStorageConfiguration.SIZE;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.util.logging.Log;

/**
 * @since 10.0
 */
public class MemoryStorageConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<MemoryStorageConfiguration>, ConfigurationBuilderInfo {

   private static final Log log = LogFactory.getLog(MemoryStorageConfigurationBuilder.class, Log.class);

   private AttributeSet attributes;
   private StorageType storageType = StorageType.OBJECT;

   MemoryStorageConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = MemoryStorageConfiguration.attributeDefinitionSet();
   }

   public MemoryStorageConfigurationBuilder storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public StorageType storageType() {
      return storageType;
   }

   public MemoryStorageConfigurationBuilder size(long size) {
      attributes.attribute(SIZE).set(size);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return MemoryConfiguration.ELEMENT_DEFINITION;
   }

   public long size() {
      return attributes.attribute(SIZE).get();
   }

   public MemoryStorageConfigurationBuilder evictionType(EvictionType type) {
      attributes.attribute(EVICTION_TYPE).set(type);
      return this;
   }

   public EvictionType evictionType() {
      return attributes.attribute(EVICTION_TYPE).get();
   }

   public MemoryStorageConfigurationBuilder evictionStrategy(EvictionStrategy strategy) {
      attributes.attribute(EVICTION_STRATEGY).set(strategy);
      return this;
   }

   public EvictionStrategy evictionStrategy() {
      return attributes.attribute(EVICTION_STRATEGY).get();
   }

   @Deprecated
   public MemoryStorageConfigurationBuilder addressCount(int addressCount) {
      attributes.attribute(ADDRESS_COUNT).set(addressCount);
      return this;
   }

   @Deprecated
   public int addressCount() {
      return attributes.attribute(ADDRESS_COUNT).get();
   }

   @Override
   public void validate() {
      if (storageType != StorageType.OBJECT) {
         if (getBuilder().clustering().hash().groups().isEnabled()) {
            throw log.groupingOnlyCompatibleWithObjectStorage(storageType);
         }
      }

      long size = attributes.attribute(SIZE).get();
      EvictionType evictionType = attributes.attribute(EVICTION_TYPE).get();
      if (evictionType == EvictionType.MEMORY) {
         if (storageType == StorageType.OBJECT) {
            throw log.offHeapMemoryEvictionNotSupportedWithObject();
         }
      }

      EvictionStrategy strategy = attributes.attribute(EVICTION_STRATEGY).get();
      if (!strategy.isEnabled()) {
         if (size > 0) {
            EvictionStrategy newStrategy = EvictionStrategy.REMOVE;
            evictionStrategy(newStrategy);
            log.debugf("Max entries configured (%d) without eviction strategy. Eviction strategy overridden to %s", size, newStrategy);
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
   public MemoryStorageConfiguration create() {
      return new MemoryStorageConfiguration(attributes.protect(), storageType);
   }

   @Override
   public MemoryStorageConfigurationBuilder read(MemoryStorageConfiguration template) {
      attributes.read(template.attributes());
      this.storageType = template.storageType();
      return this;
   }

   @Override
   public String toString() {
      return "MemoryStorageConfigurationBuilder [attributes=" + attributes + "]";
   }
}
