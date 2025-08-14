package org.infinispan.persistence.file;

import static org.infinispan.persistence.file.SingleFileStoreConfiguration.FRAGMENTATION_FACTOR;
import static org.infinispan.persistence.file.SingleFileStoreConfiguration.LOCATION;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.PersistenceUtil;

/**
 * Single file cache store configuration builder.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class SingleFileStoreConfigurationBuilder
      extends AbstractStoreConfigurationBuilder<SingleFileStoreConfiguration, SingleFileStoreConfigurationBuilder> {

   public SingleFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      this(builder, SingleFileStoreConfiguration.attributeDefinitionSet());
   }

   public SingleFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder, AttributeSet attributeSet) {
      super(builder, attributeSet);
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
   public void validate(GlobalConfiguration globalConfig) {
      PersistenceUtil.validateGlobalStateStoreLocation(globalConfig, SingleFileStore.class.getSimpleName(), attributes.attribute(LOCATION));
      super.validate(globalConfig);
   }

   @Override
   public SingleFileStoreConfiguration create() {
      return new SingleFileStoreConfiguration(attributes.protect(), async.create());
   }

   @Override
   public Builder<?> read(SingleFileStoreConfiguration template, Combine combine) {
      super.read(template, combine);
      return this;
   }

}
