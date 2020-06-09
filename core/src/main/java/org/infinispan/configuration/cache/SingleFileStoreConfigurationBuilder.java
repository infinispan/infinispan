package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.configuration.cache.SingleFileStoreConfiguration.FRAGMENTATION_FACTOR;
import static org.infinispan.configuration.cache.SingleFileStoreConfiguration.LOCATION;
import static org.infinispan.configuration.cache.SingleFileStoreConfiguration.MAX_ENTRIES;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.file.SingleFileStore;
import org.infinispan.util.logging.Log;

/**
 * Single file cache store configuration builder.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class SingleFileStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<SingleFileStoreConfiguration, SingleFileStoreConfigurationBuilder> implements ConfigurationBuilderInfo {

   private static boolean NOTIFIED_SEGMENTED;

   public SingleFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      this(builder, SingleFileStoreConfiguration.attributeDefinitionSet());
   }

   public SingleFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributeSet) {
      super(builder, attributeSet);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return SingleFileStoreConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public SingleFileStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * Sets a location on disk where the store can write.
    */
   public SingleFileStoreConfigurationBuilder location(String location) {
      attributes.attribute(LOCATION).set(location);
      return this;
   }

   /**
    * In order to speed up lookups, the single file cache store keeps an index
    * of keys and their corresponding position in the file. To avoid this
    * index resulting in memory consumption problems, this cache store can
    * bounded by a maximum number of entries that it stores. If this limit is
    * exceeded, entries are removed permanently using the LRU algorithm both
    * from the in-memory index and the underlying file based cache store.
    *
    * So, setting a maximum limit only makes sense when Infinispan is used as
    * a cache, whose contents can be recomputed or they can be retrieved from
    * the authoritative data store.
    *
    * If this maximum limit is set when the Infinispan is used as an
    * authoritative data store, it could lead to data loss, and hence it's
    * not recommended for this use case.
    */
   public SingleFileStoreConfigurationBuilder maxEntries(int maxEntries) {
      attributes.attribute(MAX_ENTRIES).set(maxEntries);
      return this;
   }

   /**
    * The store tries to fit in a new entry into an existing entry from a free entry pool (if one is available)
    * However, this existing free entry may be quite bigger than what is required to contain the new entry
    * It may then make sense to split the free entry into two parts:
    * 1. That is required to contain the new entry requested
    * 2. the remaining part to be returned to the pool of free entries.
    * The fragmentationFactor decides when to split the free entry.
    * So, if this value is set as 0.75, then the free entry will be split if the new entry is equal to or less than 0.75 times the size of free entry
    */
   public SingleFileStoreConfigurationBuilder fragmentationFactor(float fragmentationFactor) {
      attributes.attribute(FRAGMENTATION_FACTOR).set(fragmentationFactor);
      return this;
   }

   @Override
   public void validate() {
      Attribute<Boolean> segmentedAttribute = attributes.attribute(SEGMENTED);
      if ((!segmentedAttribute.isModified() || segmentedAttribute.get()) && !NOTIFIED_SEGMENTED) {
         NOTIFIED_SEGMENTED = true;
         Log.CONFIG.segmentedStoreUsesManyFileDescriptors(SingleFileStore.class.getSimpleName());
      }
      super.validate();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      PersistenceUtil.validateGlobalStateStoreLocation(globalConfig, SingleFileStore.class.getSimpleName(), attributes.attribute(LOCATION));
      super.validate(globalConfig);
   }

   @Override
   public SingleFileStoreConfiguration create() {
      return new SingleFileStoreConfiguration(attributes.protect(), async.create());
   }

   @Override
   public Builder<?> read(SingleFileStoreConfiguration template) {
      super.read(template);
      return this;
   }

}
